import Atomics

struct UnsafeWordLock {
  private var state: UnsafeMutablePointer<UInt.AtomicRepresentation>

  init(state: UnsafeMutablePointer<UInt.AtomicRepresentation>) {
    self.state = state
  }

  @inlinable
  public func lock() {
    if
      UInt.AtomicRepresentation.atomicWeakCompareExchange(
        expected: 0,
        desired: Self.LOCKED_BIT,
        at: self.state,
        successOrdering: .acquiring,
        failureOrdering: .relaxed)
      .exchanged
    {
      return
    }
    return self.lockSlow()
  }

  /// Must not be called on an already unlocked `WordLock`!
  @inlinable
  public func unlock() {
    let state = UInt.AtomicRepresentation.atomicLoadThenWrappingDecrement(at: self.state, ordering: .releasing)
    if state.isQueueLocked || state.queueHead == nil {
      return
    }
    return self.unlockSlow()
  }
}

extension UnsafeWordLock {
  fileprivate static var LOCKED_BIT: UInt { 1 }
  fileprivate static var QUEUE_LOCKED_BIT: UInt { 2 }
  fileprivate static var QUEUE_MASK: UInt { ~3 }
}

extension UnsafeWordLock {
  @inline(never)
  private func lockSlow() {
    var spinwait = SpinWait()
    var state = UInt.AtomicRepresentation.atomicLoad(at: self.state, ordering: .relaxed)
    while true {
      // Grab the lock if it isn't locked, even if there is a queue on it
      if !state.isLocked {
        let result = UInt.AtomicRepresentation.atomicWeakCompareExchange(
          expected: state,
          desired: state | Self.LOCKED_BIT,
          at: self.state,
          successOrdering: .acquiring,
          failureOrdering: .relaxed)
        guard result.exchanged else {
          state = result.original
          continue
        }

        return
      }

      // If there is no queue, try spinning a few times
      if state.queueHead == nil && spinwait.spin() {
        state = UInt.AtomicRepresentation.atomicLoad(at: self.state, ordering: .relaxed)
        continue
      }

      // Get our thread data and prepare it for parking
      state = withRawThreadLocalData { threadData in
        do {
          ThreadData.Handle(rawValue: threadData).parker.preparePark()
        }

        // Add our thread to the front of the queue
        let queueHead = state.queueHead
        if queueHead == nil {
          threadData.pointee.queueTail = threadData
          threadData.pointee.prev = nil
        } else {
          threadData.pointee.queueTail = nil
          threadData.pointee.prev = nil
          threadData.pointee.next = queueHead
        }

        let result = UInt.AtomicRepresentation.atomicWeakCompareExchange(
          expected: state,
          desired: state.withQueueHead(threadData),
          at: self.state,
          successOrdering: .acquiringAndReleasing,
          failureOrdering: .relaxed)
        guard result.exchanged else {
          return result.original
        }

        // Sleep until we are woken up by an unlock
        // Ignoring unused unsafe, since it's only a few platforms where this is unsafe.
        ThreadData.Handle(rawValue: threadData).parker.park()

        // Loop back and try locking again
        spinwait.reset()
        return UInt.AtomicRepresentation.atomicLoad(at: self.state, ordering: .relaxed)
      }
    }
  }

  @inline(never)
  func unlockSlow() {
    var state = UInt.AtomicRepresentation.atomicLoad(at: self.state, ordering: .relaxed)
    while true {
      // We just unlocked the WordLock. Just check if there is a thread
      // to wake up. If the queue is locked then another thread is already
      // taking care of waking up a thread.
      if state.isQueueLocked || state.queueHead == nil {
        return
      }

      // Try to grab the queue lock
      let result = UInt.AtomicRepresentation.atomicWeakCompareExchange(
        expected: state,
        desired: state | Self.QUEUE_LOCKED_BIT,
        at: self.state,
        successOrdering: .acquiring,
        failureOrdering: .relaxed)
      guard result.exchanged else {
        state = result.original
        continue
      }

      break
    }

    // Now we have the queue lock and the queue is non-empty
    OUTER_LOOP: while true {
      // First, we need to fill in the prev pointers for any newly added
      // threads. We do this until we reach a node that we previously
      // processed, which has a non-null queueTail pointer.
      let queueHead = state.queueHead
      var queueTail: UnsafeMutablePointer<ThreadData>? = nil
      var current = queueHead
      while true {
        queueTail = current!.pointee.queueTail
        if queueTail != nil {
          break
        }
        do {
          let next = current!.pointee.next
          next!.pointee.prev = current
          current = next
        }
      }

      // Set queueTail on the queue head to indicate that the whole list
      // has prev pointers set correctly.
      do {
        queueHead?.pointee.queueTail = queueTail
      }

      // If the WordLock is locked, then there is no point waking up a
      // thread now. Instead we let the next unlocker take care of waking
      // up a thread.
      if state.isLocked {
        let result = UInt.AtomicRepresentation.atomicWeakCompareExchange(
          expected: state,
          desired: state & ~Self.QUEUE_LOCKED_BIT,
          at: self.state,
          successOrdering: .releasing,
          failureOrdering: .relaxed)
        guard result.exchanged else {
          state = result.original

          // Need an acquire fence before reading the new queue
          fenceAcquire(self.state)
          continue
        }

        return
      }

      // Remove the last thread from the queue and unlock the queue
      let newTail = queueTail!.pointee.prev
      if newTail == nil {
        while true {
          let result = UInt.AtomicRepresentation.atomicWeakCompareExchange(
            expected: state,
            desired: state & Self.LOCKED_BIT,
            at: self.state,
            successOrdering: .releasing,
            failureOrdering: .relaxed)
          guard result.exchanged else {
            state = result.original
            // If the compare_exchange failed because a new thread was
            // added to the queue then we need to re-scan the queue to
            // find the previous element.
            if state.queueHead == nil {
              continue
            } else {
              // Need an acquire fence before reading the new queue
              fenceAcquire(self.state)
              continue OUTER_LOOP
            }
          }

          break
        }
      } else {
        do {
          queueHead?.pointee.queueTail = newTail
        }
        UInt.AtomicRepresentation.atomicLoadThenBitwiseAnd(
          with: ~Self.QUEUE_LOCKED_BIT,
          at: self.state,
          ordering: .releasing)
      }

      // Finally, wake up the thread we removed from the queue. Note that
      // we don't need to worry about any races here since the thread is
      // guaranteed to be sleeping right now and we are the only one who
      // can wake it up.
      do {
        ThreadParker.Handle(rawValue: queueTail.unsafelyUnwrapped.pointer(to: \.parker)!).unparkLock().unpark()
      }
      break
    }
  }
}

fileprivate struct ThreadData {
  var parker: ThreadParker

  // Linked list of threads in the queue. The queue is split into two parts:
  // the processed part and the unprocessed part. When new nodes are added to
  // the list, they only have the next pointer set, and queueTail is null.
  //
  // Nodes are processed with the queue lock held, which consists of setting
  // the prev pointer for each node and setting the queueTail pointer on the
  // first processed node of the list.
  //
  // This setup allows nodes to be added to the queue without a lock, while
  // still allowing O(1) removal of nodes from the processed part of the list.
  // The only cost is the O(n) processing, but this only needs to be done
  // once for each node, and therefore isn't too expensive.
  var queueTail: UnsafeMutablePointer<ThreadData>?
  var prev: UnsafeMutablePointer<ThreadData>?
  var next: UnsafeMutablePointer<ThreadData>?

  init() {
    assert(MemoryLayout<ThreadData>.alignment > ~UnsafeWordLock.QUEUE_MASK)
    self.parker = ThreadParker()
    self.queueTail = nil
    self.prev = nil
    self.next = nil
  }
}

extension ThreadData {
  struct Handle {
    var rawValue: UnsafeMutablePointer<ThreadData>

    var parker: ThreadParker.Handle {
      get { ThreadParker.Handle(rawValue: self.rawValue.pointer(to: \.parker)!) }
    }

    var queueTail: UnsafeMutablePointer<ThreadData>? {
      get { self.rawValue.pointee.queueTail }
      set { self.rawValue.pointee.queueTail = newValue }
    }
  }
}

fileprivate protocol LockState {
  var isLocked: Bool { get }
  var isQueueLocked: Bool { get }
  var queueHead: UnsafeMutablePointer<ThreadData>? { get }
  func withQueueHead(_ threadData: UnsafeMutablePointer<ThreadData>?) -> Self
}

extension UInt: LockState {
  fileprivate var isLocked: Bool {
    self & UnsafeWordLock.LOCKED_BIT != 0
  }

  fileprivate var isQueueLocked: Bool {
    self & UnsafeWordLock.QUEUE_LOCKED_BIT != 0
  }

  fileprivate var queueHead: UnsafeMutablePointer<ThreadData>? {
    UnsafeMutableRawPointer(bitPattern: Int(bitPattern: self & UnsafeWordLock.QUEUE_MASK))?
      .bindMemory(to: ThreadData.self, capacity: 1)
  }

  fileprivate func withQueueHead(_ threadData: UnsafeMutablePointer<ThreadData>?) -> Self {
    (self & ~UnsafeWordLock.QUEUE_MASK) | UInt(bitPattern: threadData)
  }
}

private let threadData = ThreadLocal {
  ThreadData()
}

fileprivate func withRawThreadLocalData<T>(_ action: (UnsafeMutablePointer<ThreadData>) -> T) -> T {
  return threadData.withValue { threadDataPtr in
    return action(threadDataPtr)
  }
}



// Thread-Sanitizer only has partial fence support, so when running under it, we
// try and avoid false positives by using a discarded acquire load instead.
private func fenceAcquire(_ a: UnsafeMutablePointer<UInt.AtomicRepresentation>) {
  _ = UInt.AtomicRepresentation.atomicLoad(at: a, ordering: .acquiring)
}

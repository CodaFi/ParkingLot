import Atomics

struct UnsafeRWLockCore {
  // There is at least one thread in the main queue.
  @inline(__always) fileprivate static var PARKED_BIT: UInt { 0b0001 }
  // There is a parked thread holding WRITER_BIT. WRITER_BIT must be set.
  @inline(__always) fileprivate static var WRITER_PARKED_BIT: UInt { 0b0010 }
  // A reader is holding an upgradable lock. The reader count must be non-zero and
  // WRITER_BIT must not be set.
  @inline(__always) fileprivate static var UPGRADABLE_BIT: UInt { 0b0100 }
  // If the reader count is zero: a writer is currently holding an exclusive lock.
  // Otherwise: a writer is waiting for the remaining readers to exit the lock.
  @inline(__always) fileprivate static var WRITER_BIT: UInt { 0b1000 }
  // Mask of bits used to count readers.
  @inline(__always) fileprivate static var READERS_MASK: UInt { ~0b1111 }
  // Base unit for counting readers.
  @inline(__always) fileprivate static var ONE_READER: UInt { 0b10000 }

  fileprivate init() {}
}

extension UnsafeRWLockCore: RWLockCore {
  typealias Core = UnsafeMutablePointer<UnsafeAtomic<UInt>.Storage>
  typealias WrappedCore = UnsafeAtomic<UInt>

  static func lockShared(_ state: Self.Core) {
    guard self.tryLockSharedFast(state, recursive: false) else {
      let result = self.lockSharedSlow(state, recursive: false)
      assert(result)
      return
    }
  }

  static func unlockShared(_ core: Self.Core) {
    let state = Self.WrappedCore(at: core).loadThenWrappingDecrement(by: Self.ONE_READER, ordering: .releasing)
    guard state & (Self.READERS_MASK | Self.WRITER_PARKED_BIT) == (Self.ONE_READER | Self.WRITER_PARKED_BIT) else {
      return
    }
    return self.unlockSharedSlow(core)
  }

  static func tryLockShared(_ state: Self.Core) -> Bool {
    if self.tryLockSharedFast(state, recursive: false) {
      return true
    }

    return self.tryLockSharedSlow(state, recursive: false)
  }

  static func lockExclusive(_ state: Self.Core) {
    if
      Self.WrappedCore(at: state)
        .weakCompareExchange(expected: 0, desired: Self.WRITER_BIT, successOrdering: .acquiring, failureOrdering: .relaxed)
        .exchanged
    {
      return
    }

    let result = self.lockExclusiveSlow(state)
    assert(result)
  }

  static func tryLockExclusive(_ state: Self.Core) -> Bool {
    return Self.WrappedCore(at: state)
      .compareExchange(expected: 0, desired: Self.WRITER_BIT, successOrdering: .acquiring, failureOrdering: .relaxed)
      .exchanged
  }

  static func unlockExclusive(_ state: Self.Core) {
    if
      Self.WrappedCore(at: state)
        .compareExchange(expected: Self.WRITER_BIT, desired: 0, successOrdering: .releasing, failureOrdering: .relaxed)
        .exchanged
    {
      return
    }

    return self.unlockExclusiveSlow(state)
  }
}

extension UnsafeRWLockCore {
  static func tryLockSharedFast(_ core: Self.Core, recursive: Bool) -> Bool {
    let state = Self.WrappedCore(at: core).load(ordering: .relaxed)

    // We can't allow grabbing a shared lock if there is a writer, even if
    // the writer is still waiting for the remaining readers to exit.
    if state & Self.WRITER_BIT != 0 {
      // To allow recursive locks, we make an exception and allow readers
      // to skip ahead of a pending writer to avoid deadlocking, at the
      // cost of breaking the fairness guarantees.
      if !recursive || state & Self.READERS_MASK == 0 {
        return false
      }
    }

    let (value, overflow) = state.addingReportingOverflow(Self.ONE_READER)
    guard !overflow else  {
      return false
    }

    return Self.WrappedCore(at: core)
      .weakCompareExchange(expected: state, desired: value, successOrdering: .acquiring, failureOrdering: .relaxed)
      .exchanged
  }


  static func tryLockSharedSlow(_ core: Self.Core, recursive: Bool) -> Bool {
    var state = Self.WrappedCore(at: core).load(ordering: .relaxed)
    while true {
      // This mirrors the condition in tryLockSharedFast
      if state & WRITER_BIT != 0 {
        if !recursive || state & READERS_MASK == 0 {
          return false
        }
      }

      let (exchanged, value) = Self.WrappedCore(at: core)
        .weakCompareExchange(expected: state, desired: state + Self.ONE_READER, successOrdering: .acquiring, failureOrdering: .relaxed)
      if exchanged {
        return true
      }

      state = value
    }
  }

  static func lockSharedSlow(
    _ core: Self.Core,
    recursive: Bool
    //    timeout: Option<Instant>
  ) -> Bool {
    return self.lockCommon(
      core, token: .shared, validateFlags: Self.WRITER_BIT) { state in
        var spinwaitShared = SpinWait()
        while true {
          // This is the same condition as tryLockSharedFast
          if state & WRITER_BIT != 0 {
            if !recursive || state & READERS_MASK == 0 {
              return false
            }
          }

          if Self.WrappedCore(at: core)
            .weakCompareExchange(expected: state, desired: state + 1, successOrdering: .acquiring, failureOrdering: .relaxed)
            .exchanged
          {
            return true
          }

          // If there is high contention on the reader count then we want
          // to leave some time between attempts to acquire the lock to
          // let other threads make progress.
          spinwaitShared.spinWithoutYielding()
          state = Self.WrappedCore(at: core).load(ordering: .relaxed)
        }
      }
  }
}

extension UnsafeRWLockCore {
  static func lockExclusiveSlow(_ state: Self.Core) -> Bool {
    // Step 1: grab exclusive ownership of WRITER_BIT
    let timed_out = !self.lockCommon(
      state,
      token: .exclusive,
      validateFlags: Self.WRITER_BIT | Self.UPGRADABLE_BIT) { curState in
        while true {
          if curState & (WRITER_BIT | UPGRADABLE_BIT) != 0 {
            return false
          }

          // Grab WRITER_BIT if it isn't set, even if there are parked threads.
          let (exchanged, oldValue) = Self.WrappedCore(at: state)
            .weakCompareExchange(expected: curState, desired: curState | Self.WRITER_BIT, successOrdering: .acquiring, failureOrdering: .relaxed)
          if exchanged {
            return true
          }

          curState = oldValue
        }
      }

    if timed_out {
      return false
    }

    // Step 2: wait for all remaining readers to exit the lock.
    return self.waitForReaders(state, previousValue: 0)
  }

  static func unlockExclusiveSlow(_ core: Self.Core) {
    // There are threads to unpark. Try to unpark as many as we can.
    return self.wakeParkedThreads(core, newState: 0) { newState, result in
      var newState = newState
      // If we are using a fair unlock then we should keep the
      // rwlock locked and hand it off to the unparked threads.
      if result.unparkedThreads != 0 {
        if result.haveMoreThreads {
          newState |= PARKED_BIT
        }
        Self.WrappedCore(at: core).store(newState, ordering: .releasing)
        return .handOff
      } else {
        // Clear the parked bit if there are no more parked threads.
        if result.haveMoreThreads {
          Self.WrappedCore(at: core).store(Self.PARKED_BIT, ordering: .releasing)
        } else {
          Self.WrappedCore(at: core).store(0, ordering: .releasing)
        }
        return .normal
      }
    }
  }
}

extension UnsafeRWLockCore {
  static func unlockSharedSlow(_ core: Self.Core) {
    // At this point WRITER_PARKED_BIT is set and READER_MASK is empty. We
    // just need to wake up a potentially sleeping pending writer.
    // Using the 2nd key at addr + 1

    // SAFETY:
    //   * `addr` is an address we control.
    //   * `callback` does not call into any function of `parking_lot`.
    _ = ParkingLot.unparkOne(UInt(bitPattern: core)) { _ in
      // Clear the WRITER_PARKED_BIT here since there can only be one
      // parked writer thread.
      _ = Self.WrappedCore(at: core).loadThenBitwiseAnd(with: ~Self.WRITER_PARKED_BIT, ordering: .relaxed)
      return .normal
    }
  }
}

extension UnsafeRWLockCore {
  /// Common code for acquiring a lock
  @inline(__always)
  static func lockCommon(
    _ core: Self.Core,
    token: HashTable.Bucket.ThreadData.ParkToken,
    validateFlags: UInt,
    tryLock: (inout UInt) -> Bool
  ) -> Bool {
    var spinwait = SpinWait()
    let coreState = Self.WrappedCore(at: core)
    var state = coreState.load(ordering: .relaxed)
    while true {
      // Attempt to grab the lock
      if tryLock(&state) {
        return true
      }

      // If there are no parked threads, try spinning a few times.
      if state & (PARKED_BIT | WRITER_PARKED_BIT) == 0 && spinwait.spin() {
        state = coreState.load(ordering: .relaxed)
        continue
      }

      // Set the parked bit
      if state & PARKED_BIT == 0 {
        let (updatedValue, exchanged) = coreState
          .weakCompareExchange(expected: state, desired: state | Self.PARKED_BIT, successOrdering: .relaxed, failureOrdering: .relaxed)
        guard updatedValue else {
          state = exchanged
          continue
        }
      }

      // Park our thread until we are woken up by an unlock
      //
      // SAFETY:
      // * `addr` is an address we control.
      // * `validate` does not call into any function of `ParkingLot`.
      let parkResult = ParkingLot.park(UInt(bitPattern: core), token: token) {
        let state = coreState.load(ordering: .relaxed)
        return (state & Self.PARKED_BIT != 0) && (state & validateFlags != 0)
      }

      switch parkResult {
      case .unparked(HashTable.Bucket.ThreadData.UnparkToken.handOff):
        // The thread that unparked us passed the lock on to us
        // directly without unlocking it.
        return true
      case .unparked(_), .invalid:
        // We were unparked normally, try acquiring the lock again
        // The validation function failed, try locking again
        break
      }

      // Loop back and try locking again
      spinwait.reset()
      state = coreState.load(ordering: .relaxed)
    }
  }

  // Common code for waiting for readers to exit the lock after acquiring
  // WRITER_BIT.
  @inline(__always)
  static func waitForReaders(
    _ core: Self.Core,
    previousValue: UInt
  ) -> Bool {
    // At this point WRITER_BIT is already set, we just need to wait for the
    // remaining readers to exit the lock.
    var spinwait = SpinWait()
    let coreState = Self.WrappedCore(at: core)
    var state = coreState.load(ordering: .acquiring)
    while state & READERS_MASK != 0 {
      // Spin a few times to wait for readers to exit
      if spinwait.spin() {
        state = coreState.load(ordering: .acquiring)
        continue
      }

      // Set the parked bit
      if state & WRITER_PARKED_BIT == 0 {
        let (exchanged, oldValue) = coreState
          .weakCompareExchange(expected: state, desired: state | Self.WRITER_PARKED_BIT, successOrdering: .acquiring, failureOrdering: .acquiring)
        guard exchanged else {
          state = oldValue
          continue
        }
      }

      // Park our thread until we are woken up by an unlock
      // Using the 2nd key at addr + 1
      //
      // SAFETY:
      //   * `addr` is an address we control.
      //   * `validate` does not call into any function of `parking_lot`.
      let parkResult = ParkingLot.park(UInt(bitPattern: core) + 1, token: .exclusive) {
        let state = coreState.load(ordering: .relaxed)
        return state & READERS_MASK != 0 && state & WRITER_PARKED_BIT != 0
      }

      switch parkResult {
      case .unparked(_), .invalid:
        // We still need to re-check the state if we are unparked
        // since a previous writer timing-out could have allowed
        // another reader to sneak in before we parked.
        state = coreState.load(ordering: .acquiring)
        continue
      }
    }
    return true
  }

  @inline(__always)
  private static func wakeParkedThreads(
    _ core: Self.Core,
    newState: UInt,
    callback: (UInt, UnparkResult) -> HashTable.Bucket.ThreadData.UnparkToken
  ) {
    // We must wake up at least one upgrader or writer if there is one,
    // otherwise they may end up parked indefinitely since unlockShared
    // does not call wakeParkedThreads.
    _ = withUnsafeTemporaryAllocation(of: UInt.self, capacity: 1) { newStateCell in
      newStateCell.baseAddress?.initialize(to: newState)

      // SAFETY:
      // * `addr` is an address we control.
      // * `filter` does not call into any function of `ParkingLot`.
      // * `callback` safety responsibility is on caller
      return ParkingLot.unparkFilter(key: UInt(bitPattern: core)) { token in
          let s = newStateCell[0]

          // If we are waking up a writer, don't wake anything else.
          if s & WRITER_BIT != 0 {
            return .stop
          }

          // Otherwise wake *all* readers and one upgrader/writer.
          if token.rawValue & (UPGRADABLE_BIT | WRITER_BIT) != 0 && s & UPGRADABLE_BIT != 0 {
            // Skip writers and upgradable readers if we already have
            // a writer/upgradable reader.
            return .skip
          } else {
            newStateCell[0] = s + token.rawValue
            return .unpark
          }
        } callback: { result in
          return callback(newStateCell[0], result)
        }
    }
  }
}

extension HashTable.Bucket.ThreadData.UnparkToken {
  // UnparkToken used to indicate that that the target thread should attempt to
  // lock the mutex again as soon as it is unparked.
  static let normal: Self = Self(rawValue: 0)

  // UnparkToken used to indicate that the mutex is being handed off to the target
  // thread directly without unlocking it.
  static let handOff: Self = Self(rawValue: 1)
}

extension HashTable.Bucket.ThreadData.ParkToken {
  // Token indicating what type of lock a queued thread is trying to acquire
  static let shared: Self = Self(rawValue: UnsafeRWLockCore.ONE_READER)
  static let exclusive: Self = Self(rawValue: UnsafeRWLockCore.WRITER_BIT)
  static let upgradable: Self = Self(rawValue: UnsafeRWLockCore.ONE_READER | UnsafeRWLockCore.UPGRADABLE_BIT)
}

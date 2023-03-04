import Atomics

extension HashTable.Bucket {
  struct ThreadData {
    static var threadCount: UnsafeAtomic<UInt> = .create(0)

    struct UnparkToken: Equatable, Hashable {
      var rawValue: UInt
    }

    struct ParkToken: Equatable, Hashable {
      var rawValue: UInt
    }

    var parker: ThreadParker

    // Key that this thread is sleeping on. This may change if the thread is
    // requeued to a different key.
    var key: UInt.AtomicRepresentation

    // Linked list of parked threads in a bucket
    var nextInQueue: UnsafeMutablePointer<ThreadData>?

    // UnparkToken passed to this thread when it is unparked
    var unparkToken: UnparkToken

    // ParkToken value set by the thread when it was parked
    var parkToken: ParkToken

    init() {
      // Keep track of the total number of live ThreadData objects and resize
      // the hash table accordingly.
      let numThreads = Self.threadCount.loadThenWrappingIncrement(ordering: .relaxed) + 1
      HashTable.growHashtable(numThreads)

      self.parker = ThreadParker()
      self.key = UInt.AtomicRepresentation(0)
      self.nextInQueue = nil
      self.unparkToken = UnparkToken(rawValue: 0)
      self.parkToken = ParkToken(rawValue: 0)
    }
  }
}

extension HashTable.Bucket {
  struct Handle {
    var rawValue: UnsafeMutablePointer<HashTable.Bucket>

    @inline(__always)
    var mutex: UnsafeWordLock {
      return UnsafeWordLock(state: self.rawValue.pointer(to: \.mutex).unsafelyUnwrapped)
    }

    @inline(__always)
    var queueHead: HashTable.Bucket.ThreadData.Handle? {
      get { self.rawValue.pointee.queueHead.map(HashTable.Bucket.ThreadData.Handle.init) }
      set { self.rawValue.pointee.queueHead = newValue?.rawValue }
    }

    @inline(__always)
    var queueTail: HashTable.Bucket.ThreadData.Handle? {
      get { self.rawValue.pointee.queueTail.map(HashTable.Bucket.ThreadData.Handle.init) }
      set { self.rawValue.pointee.queueTail = newValue?.rawValue }
    }
  }
}

extension HashTable.Bucket.ThreadData {
  struct Handle {
    var rawValue: UnsafeMutablePointer<HashTable.Bucket.ThreadData>

    var key: UnsafeMutablePointer<UInt.AtomicRepresentation> {
      return self.rawValue.pointer(to: \.key).unsafelyUnwrapped
    }

    @inline(__always)
    var parker: ThreadParker.Handle {
      return ThreadParker.Handle(rawValue: self.rawValue.pointer(to: \.parker).unsafelyUnwrapped)
    }

    // Linked list of parked threads in a bucket
    @inline(__always)
    var nextInQueue: HashTable.Bucket.ThreadData.Handle? {
      get { self.rawValue.pointee.nextInQueue.map(HashTable.Bucket.ThreadData.Handle.init) }
      set { self.rawValue.pointee.nextInQueue = newValue?.rawValue }
    }

    // UnparkToken passed to this thread when it is unparked
    @inline(__always)
    var unparkToken: UnsafeMutablePointer<UnparkToken> {
      self.rawValue.pointer(to: \.unparkToken).unsafelyUnwrapped
    }

    // ParkToken value set by the thread when it was parked
    @inline(__always)
    var parkToken: UnsafeMutablePointer<ParkToken> {
      self.rawValue.pointer(to: \.parkToken).unsafelyUnwrapped
    }
  }
}


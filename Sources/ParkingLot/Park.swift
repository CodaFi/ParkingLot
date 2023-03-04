enum ParkResult {
  /// We were unparked by another thread with the given token.
  case unparked(HashTable.Bucket.ThreadData.UnparkToken)

  /// The validation callback returned false.
  case invalid
}

@inline(__always)
func park(
  _ key: UInt,
  token parkToken: HashTable.Bucket.ThreadData.ParkToken,
  validate: () -> Bool
) -> ParkResult {
  // Grab our thread data, this also ensures that the hash table exists
  return withThreadLocalHashTableData { threadData in
    // Lock the bucket for the given key
    var bucket = HashTable.get().pointee.lockBucket(key)

    // If the validation function fails, just return
    guard validate() else {
      // SAFETY: We hold the lock here, as required
      bucket.mutex.unlock()
      return .invalid
    }

    // Append our thread data to the queue and unlock the bucket
    threadData.nextInQueue = nil
    UInt.AtomicRepresentation.atomicStore(
      key, at: threadData.key, ordering: .relaxed)
    threadData.parkToken.deinitialize(count: 1)
    threadData.parkToken.initialize(to: parkToken)
    threadData.parker.preparePark()
    if bucket.queueHead != nil {
      bucket.queueTail?.nextInQueue?.rawValue = threadData.rawValue
    } else {
      bucket.queueHead = threadData
    }
    bucket.queueTail = threadData
    // SAFETY: We hold the lock here, as required
    bucket.mutex.unlock()

    // Park our thread and determine whether we were woken up by an unpark
    // or by our timeout. Note that this isn't precise: we can still be
    // unparked since we are still in the queue.
    threadData.parker.park()

    return .unparked(threadData.unparkToken.move())
  }
}

private let threadData = ThreadLocal {
  HashTable.Bucket.ThreadData()
}

@inline(__always)
fileprivate func withThreadLocalHashTableData<T>(_ f: (inout HashTable.Bucket.ThreadData.Handle) -> T) -> T {
  return threadData.withValue { threadDataPtr in
    var handle = HashTable.Bucket.ThreadData.Handle(rawValue: threadDataPtr)
    return f(&handle)
  }
}

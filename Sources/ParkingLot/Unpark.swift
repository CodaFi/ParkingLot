public struct UnparkResult {
  /// The number of threads that were unparked.
  public var unparkedThreads: Int

  /// Whether there are any threads remaining in the queue. This only returns
  /// true if a thread was unparked.
  public var haveMoreThreads: Bool

  init() {
    self.unparkedThreads = 0
    self.haveMoreThreads = false
  }
}


/// Unparks one thread from the queue associated with the given key.
///
/// The `callback` function is called while the queue is locked and before the
/// target thread is woken up. The `UnparkResult` argument to the function
/// indicates whether a thread was found in the queue and whether this was the
/// last thread in the queue. This value is also returned by ``ParkingLot/unparkOne``.
///
/// The `callback` function should return an `UnparkToken` value which will be
/// passed to the thread that is unparked. If no thread is unparked then the
/// returned value is ignored.
///
/// # Safety
///
/// You should only call this function with an address that you control, since
/// you could otherwise interfere with the operation of other synchronization
/// primitives.
///
/// The `callback` function is called while the queue is locked and must not
/// call into any function in `ParkingLot`.
@inline(__always)
func unparkOne(
  _ key: UInt,
  callback: (UnparkResult) -> HashTable.Bucket.ThreadData.UnparkToken
) -> UnparkResult {
  // Lock the bucket for the given key
  let bucket = HashTable.get().pointee.lockBucket(key)

  // Find a thread with a matching key and remove it from the queue
  var link = bucket.queueHead
  var current = bucket.queueHead
  var previous: HashTable.Bucket.ThreadData.Handle? = nil
  var result = UnparkResult()
  while current != nil {
    if UInt.AtomicRepresentation.atomicLoad(at: current!.key, ordering: .relaxed) == key {
      // Remove the thread from the queue
      let next = current!.nextInQueue
      if let nextValue = next?.rawValue {
        link!.rawValue = nextValue
      }
      if bucket.queueTail?.rawValue == current?.rawValue {
        bucket.rawValue.pointee.queueTail = previous?.rawValue
      } else {
        // Scan the rest of the queue to see if there are any other
        // entries with the given key.
        var scan = next
        while scan != nil {
          if UInt.AtomicRepresentation.atomicLoad(at: scan!.key, ordering: .relaxed) == key {
            result.haveMoreThreads = true
            break
          }
          scan = scan!.nextInQueue
        }
      }

      // Invoke the callback before waking up the thread
      result.unparkedThreads = 1
      let token = callback(result)

      // Set the token for the target thread
      current!.unparkToken.pointee = token

      // This is a bit tricky: we first lock the ThreadParker to prevent
      // the thread from exiting and freeing its ThreadData if its wait
      // times out. Then we unlock the queue since we don't want to keep
      // the queue locked while we perform a system call. Finally we wake
      // up the parked thread.
      let handle = current!.parker.unparkLock()
      // SAFETY: We hold the lock here, as required
      bucket.mutex.unlock()
      handle.unpark()

      return result
    } else {
      link = current?.nextInQueue
      previous = current
      current = link
    }
  }

  // No threads with a matching key were found in the bucket
  _ = callback(result)
  // SAFETY: We hold the lock here, as required
  bucket.mutex.unlock()

  return result
}

enum UnparkFilter {
  /// Unpark the thread and continue scanning the list of parked threads.
  case unpark

  /// Don't unpark the thread and continue scanning the list of parked threads.
  case skip

  /// Don't unpark the thread and stop scanning the list of parked threads.
  case stop
}

@inline(__always)
func unparkFilter(
  key: UInt,
  filter: (HashTable.Bucket.ThreadData.ParkToken) -> UnparkFilter,
  callback: (UnparkResult) -> HashTable.Bucket.ThreadData.UnparkToken
) -> UnparkResult {
  // Lock the bucket for the given key
  let bucket = HashTable.get().pointee.lockBucket(key)

  // Go through the queue looking for threads with a matching key
  var link = bucket.queueHead
  var current = bucket.queueHead
  var previous: HashTable.Bucket.ThreadData.Handle? = nil
  var threads = [(HashTable.Bucket.ThreadData.Handle?, ThreadParker.UnparkHandle?)]()
  threads.reserveCapacity(8)
  var result = UnparkResult()
  while current != nil {
    if UInt.AtomicRepresentation.atomicLoad(at: current!.key, ordering: .relaxed) == key {
      // Call the filter function with the thread's ParkToken
      let next = current?.nextInQueue
      switch filter(current!.parkToken.pointee) {
      case .unpark:
        // Remove the thread from the queue
        link = next
        if bucket.queueTail?.rawValue == current?.rawValue {
          bucket.rawValue.pointee.queueTail = previous?.rawValue
        }

        // Add the thread to our list of threads to unpark
        threads.append((current, nil))

        current = next
      case .skip:
        result.haveMoreThreads = true
        link = current?.nextInQueue
        previous = current
        current = link
      case .stop:
        result.haveMoreThreads = true
        break
      }
    } else {
      link = current?.nextInQueue
      previous = current
      current = link
    }
  }

  // Invoke the callback before waking up the threads
  result.unparkedThreads = threads.count
  let token = callback(result)

  // Pass the token to all threads that are going to be unparked and prepare
  // them for unparking.
  for ti in threads.indices {
    threads[ti].0!.unparkToken.pointee = token
    threads[ti].1 = threads[ti].0!.parker.unparkLock()
  }

  // SAFETY: We hold the lock here, as required
  bucket.mutex.unlock()

  // Now that we are outside the lock, wake up all the threads that we removed
  // from the queue.
  for (_, handle) in threads {
    handle.unsafelyUnwrapped.unpark()
  }

  return result
}


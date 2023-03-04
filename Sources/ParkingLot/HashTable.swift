import Atomics

internal struct HashTable {
  typealias UnsafeAtomicBox = UnsafeMutablePointer<HashTable>.AtomicOptionalRepresentation
  public static var global = HashTable.UnsafeAtomicBox(nil)

  // Even with 3x more buckets than threads, the memory overhead per thread is
  // still only a few hundred bytes per thread.
  @inline(__always)
  fileprivate static var LOAD_FACTOR: Int { 3 }

  @usableFromInline
  internal var _storage: Storage

  @inlinable
  @inline(__always)
  internal init(_ storage: Storage) {
    _storage = storage
  }
}

extension HashTable {
  internal final class Storage : ManagedBuffer<Header, Bucket> {}

  @usableFromInline
  internal struct Header {
    var capacity: UInt32
    var hashBits: UInt32
    weak var previous: HashTable.Storage?
  }

  struct Bucket {
    // Lock protecting the queue
    var mutex: UInt.AtomicRepresentation

    // Linked list of threads waiting on this bucket
    var queueHead: UnsafeMutablePointer<ThreadData>?
    var queueTail: UnsafeMutablePointer<ThreadData>?

    @inline(__always)
    init(_ seed: UInt32) {
      self.mutex = UInt.AtomicRepresentation(0)
      self.queueHead = nil
      self.queueTail = nil
    }
  }
}

extension HashTable {
  @usableFromInline
  @_effects(releasenone)
  internal init(threadCount: Int, previous: HashTable.Storage?) {
    let newSize = UInt(threadCount * Self.LOAD_FACTOR).nextPowerOfTwo()
    let hashBits = UInt(0).leadingZeroBitCount - newSize.leadingZeroBitCount - 1

    let storage = Storage.create(
      minimumCapacity: newSize,
      makingHeaderWith: { object in
        return Header(capacity: UInt32(newSize), hashBits: UInt32(hashBits), previous: previous)
      })
    storage.withUnsafeMutablePointerToElements { elements in
      for i in 0..<newSize {
        // We must ensure the seed is not zero
        (elements + i).initialize(to: Bucket(UInt32(i) + 1))
      }
    }
    self.init(unsafeDowncast(storage, to: Storage.self))
  }
}

extension HashTable {
  static func get() -> UnsafeMutablePointer<HashTable> {
    guard let table = HashTable.UnsafeAtomicBox.atomicLoad(at: &HashTable.global, ordering: .acquiring) else {
      let pointer = UnsafeMutablePointer<HashTable>.allocate(capacity: 1)
      pointer.initialize(to: HashTable(threadCount: LOAD_FACTOR, previous: nil))
      let (exchanged, original) = HashTable.UnsafeAtomicBox.atomicCompareExchange(
        expected: nil,
        desired: pointer,
        at: &global,
        successOrdering: .acquiringAndReleasing,
        failureOrdering: .acquiring)

      guard exchanged else {
        // Free the table we created
        pointer.deinitialize(count: 1)
        pointer.initialize(to: original!.pointee)
        return original!
      }
      return pointer
    }

    return table
  }

  func lockBucket(_ key: UInt) -> Bucket.Handle {
    while true {
      let hashtable = HashTable.get()

      let bucket = hashtable.pointee._storage.withUnsafeMutablePointers { header, buckets in
        let hash = hash(key: key, bits: header.pointee.hashBits)
        return Bucket.Handle(rawValue: buckets.advanced(by: Int(hash)))
      }

      // Lock the bucket
      bucket.mutex.lock()

      // If no other thread has rehashed the table before we grabbed the lock
      // then we are good to go! The lock we grabbed prevents any rehashes.
      if HashTable.get() == hashtable {
        return bucket
      }

      // Unlock the bucket and try again
      // SAFETY: We hold the lock here, as required
      bucket.mutex.unlock()
    }
  }
}

extension HashTable {
  static func growHashtable(_ threadCount: UInt) {
    // Lock all buckets in the existing table and get a reference to it
    guard let (oldTable, cap) = Self.claimTable(threadCount) else {
      return
    }

    // Create the new table
    let newTable = HashTable(threadCount: Int(threadCount), previous: oldTable._storage)
    let hashBits = newTable._storage.withUnsafeMutablePointerToHeader { $0.pointee.hashBits }

    // Move the entries from the old table to the new one
    oldTable._storage.withUnsafeMutablePointerToElements { oldBuckets in
      newTable._storage.withUnsafeMutablePointerToElements { newBuckets in
        for i in 0..<cap {
          // SAFETY: The park, unpark* and check_wait_graph_fast functions create only correct linked
          // lists. All `ThreadData` instances in these lists will remain valid as long as they are
          // present in the lists, meaning as long as their threads are parked.
          let bucket = Bucket.Handle(rawValue: oldBuckets + i)
          var current = bucket.queueHead
          while current != nil {
            let next = current!.nextInQueue
            let hash = hash(key: UInt.AtomicRepresentation.atomicLoad(at: current!.key, ordering: .relaxed), bits: hashBits)
            var newBucket = Bucket.Handle(rawValue: newBuckets + Int(hash))
            if newBucket.queueTail == nil {
              newBucket.queueHead = current
            } else {
              newBucket.queueTail?.nextInQueue = current
            }
            newBucket.queueTail = current
            current?.nextInQueue = nil
            current = next
          }
        }
      }
    }

    // Publish the new table. No races are possible at this point because
    // any other thread trying to grow the hash table is blocked on the bucket
    // locks in the old table.
    let allocation = UnsafeMutablePointer<HashTable>.allocate(capacity: 1)
    allocation.initialize(to: newTable)
    HashTable.UnsafeAtomicBox.atomicStore(allocation, at: &HashTable.global, ordering: .releasing)

    // Unlock all buckets in the old table
    oldTable._storage.withUnsafeMutablePointerToElements { buckets in
      for i in 0..<cap {
        Bucket.Handle(rawValue: buckets + i).mutex.unlock()
      }
    }
  }

  private static func claimTable(_ threadCount: UInt) -> (HashTable, Int)? {
    while true {
      let table = HashTable.get()

      // Check if we need to resize the existing table
      let cap = table.pointee._storage.withUnsafeMutablePointerToHeader { Int($0.pointee.capacity) }
      if UInt(cap) >= UInt(HashTable.LOAD_FACTOR) * threadCount {
        return nil
      }

      // Lock all buckets in the old table
      table.pointee._storage.withUnsafeMutablePointerToElements { buckets in
        for i in 0..<cap {
          Bucket.Handle(rawValue: buckets + i).mutex.lock()
        }
      }

      // Now check if our table is still the latest one. Another thread could
      // have grown the hash table between us reading HASHTABLE and locking
      // the buckets.
      if HashTable.get() == table {
        return (table.pointee, cap)
      }

      // Unlock buckets and try again
      table.pointee._storage.withUnsafeMutablePointerToElements { buckets in
        for i in 0..<cap {
          Bucket.Handle(rawValue: buckets + i).mutex.unlock()
        }
      }
    }
  }

}

extension UInt {
  fileprivate func nextPowerOfTwo() -> Int {
    if self <= 1 { return 1 }

    let p = self - 1
    // SAFETY: Because `p > 0`, it cannot consist entirely of leading zeros.
    // That means the shift is always in-bounds, and some processors
    // (such as intel pre-haswell) have more efficient ctlz
    // intrinsics when the argument is non-zero.
    let z = p.leadingZeroBitCount
    return Int((UInt.max >> z) + 1)
  }
}

@inline(__always)
private func hash(key: UInt, bits: UInt32) -> UInt {
  return (key &* 0x9E3779B97F4A7C15) >> (64 - bits)
}

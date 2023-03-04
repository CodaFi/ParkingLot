import Atomics

public struct RWLock<T>: @unchecked Sendable {
  let core: ManagedBuffer<T, UnsafeAtomic<UInt>.Storage>

  public init(protecting value: T) {
    self.core = ManagedBuffer.create(minimumCapacity: 1) { buf in
      value
    }
    self.core.withUnsafeMutablePointerToElements { elements in
      elements.initialize(to: .init(0))
    }
  }

  public func read<R>(_ continuation: (T) throws -> R) rethrows -> R {
    return try self.core.withUnsafeMutablePointers { header, core in
      UnsafeRWLockCore.lockShared(core)
      // SAFETY: The lock is held, as required.
      defer { UnsafeRWLockCore.unlockShared(core) }
      return try continuation(header.pointee)
    }
  }

  public func tryRead<R>(_ continuation: (T) throws -> R) rethrows -> R? {
    return try self.core.withUnsafeMutablePointers { header, core in
      guard UnsafeRWLockCore.tryLockShared(core) else {
        return nil
      }
      // SAFETY: The lock is held, as required.
      defer { UnsafeRWLockCore.unlockShared(core) }
      return try continuation(header.pointee)
    }
  }

  public func write<R>(_ continuation: (inout T) throws -> R) rethrows -> R {
    return try self.core.withUnsafeMutablePointers { header, core in
      UnsafeRWLockCore.lockExclusive(core)
      // SAFETY: The lock is held, as required.
      defer { UnsafeRWLockCore.unlockExclusive(core) }
      return try continuation(&header.pointee)
    }
  }
}

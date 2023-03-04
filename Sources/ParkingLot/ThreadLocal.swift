#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#else
#error("unsupported platform")
#endif

public struct ThreadLocal<Value> {
  private var key: Key

  private var create: () -> Value

  /// Creates an instance that will use `create` to generate an initial value.
  public init(create: @escaping () -> Value) {
    self.create = create
    self.key = Key()
  }

  /// Returns the result of the closure performed on the value of `self`.
  public func withValue<T>(_ body: (UnsafeMutablePointer<Value>) throws -> T) rethrows -> T {
    return try body(self.key.box(create: self.create))
  }
}

extension ThreadLocal {
  private final class Key {
    var raw: pthread_key_t

    func withThreadLocalValue<U>(_ access: (UnsafeMutablePointer<Value>?) throws -> U) rethrows -> U {
      guard let pointer = pthread_getspecific(self.raw) else {
        return try access(nil)
      }
      return try pointer.withMemoryRebound(to: Value.self, capacity: 1) { pointer in
        return try access(pointer)
      }
    }

    init() {
      self.raw = pthread_key_t()
      pthread_key_create(&self.raw) {
        guard let rawPointer = ($0 as UnsafeMutableRawPointer?) else {
          return
        }

        rawPointer.deallocate()
      }
    }

    deinit {
      pthread_key_delete(self.raw)
    }

    func box(create: () throws -> Value) rethrows -> UnsafeMutablePointer<Value> {
      return try self.withThreadLocalValue { buf in
        if let buf {
          return buf
        } else {
          let box = UnsafeMutablePointer<Value>.allocate(capacity: 1)
          try box.initialize(to: create())
          pthread_setspecific(self.raw, box)
          return box
        }
      }
    }
  }
}

import Atomics

public struct UnsafeRawMutex {
  private var state: UInt8.AtomicRepresentation
}

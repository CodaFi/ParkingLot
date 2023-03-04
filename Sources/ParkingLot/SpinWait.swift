@_implementationOnly import Spin
import Darwin

/// A counter used to perform exponential backoff in spin loops.
public struct SpinWait {
  @usableFromInline
  var counter: UInt32

  /// Creates a new `SpinWait`.
  @inlinable
  public init() {
    self.counter = 0
  }

  /// Resets a `SpinWait` to its initial state.
  @inlinable
  public mutating func reset() {
    self.counter = 0
  }

  /// Spins until the sleep threshold has been reached.
  ///
  /// This function returns whether the sleep threshold has been reached, at
  /// which point further spinning has diminishing returns and the thread
  /// should be parked instead.
  ///
  /// The spin strategy will initially use a CPU-bound loop but will fall back
  /// to yielding the CPU to the OS after a few iterations.
  @inlinable
  public mutating func spin() -> Bool {
    if self.counter >= 10 {
      return false
    }
    self.counter += 1
    if self.counter <= 3 {
      relax(for: 1 << self.counter)
    } else {
      sched_yield()
    }
    return true
  }

  /// Spins without yielding the thread to the OS.
  ///
  /// Instead, the backoff is simply capped at a maximum value. This can be
  /// used to improve throughput in `compare_exchange` loops that have high
  /// contention.
  @inlinable
  public mutating func spinWithoutYielding() {
    self.counter += 1
    if self.counter > 10 {
      self.counter = 10
    }
    relax(for: 1 << self.counter)
  }
}

// Wastes some CPU time for the given number of iterations,
// using a hint to indicate to the CPU that we are spinning.
@inline(__always)
@usableFromInline
func relax(for iterations: UInt32) {
  for _ in 0..<iterations {
    spin_loop()
  }
}

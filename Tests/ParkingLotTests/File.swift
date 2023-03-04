import Atomics
import ParkingLot
import XCTest
import Dispatch

final class RWLockTests: XCTestCase {
  func testSharedReferenceToLock() async {

    // Write-Read
    do {
      let arc = RWLock(protecting: 1)
      let arc2 = arc
      await withTaskGroup(of: Void.self) { group in
        group.addTask {
          arc2.write { _ in }
          return
        }
        group.addTask {
          arc.read { value in
            XCTAssertEqual(value, 1)
          }
        }
      }
    }

    // Write-Write
    do {
      let arc = RWLock(protecting: 1)
      let arc2 = arc
      await withTaskGroup(of: Void.self) { group in
        group.addTask {
          arc2.write { _ in }
          return
        }
        group.addTask {
          arc.write { value in
            XCTAssertEqual(value, 1)
          }
        }
      }
    }

    // Read-Read
    do {
      let arc = RWLock(protecting: 1)
      let arc2 = arc
      await withTaskGroup(of: Void.self) { group in
        group.addTask {
          arc2.read { _ in }
          return
        }
        group.addTask {
          arc.read { value in
            XCTAssertEqual(value, 1)
          }
        }
      }
    }
  }

  func testTryRead() {
    let lock = RWLock(protecting: 0)
    lock.read { _ in
      XCTAssertNotNil(lock.tryRead({ $0 }))
    }
    lock.write { _ in
      XCTAssertNil(lock.tryRead({ $0 }))
    }
  }

  private func runBenchmark(
    secondsPerTest: Int,
    numWriterThreads: Int,
    numReaderThreads: Int,
    workPerCriticalSection: Int,
    workBetweenCriticalSections: Int
  ) {
    let lock = RWLock(protecting: ([Int](repeating: 0, count: 300), Double(0.0), [Int](repeating: 0, count: 300)))
    let keepGoing = ManagedAtomic<Bool>(true)
    let readerBarrier = DispatchGroup()
    let writerBarrier = DispatchGroup()
    //    var writers = []
    //    var readers = []

    for _ in 0..<numWriterThreads {
      readerBarrier.enter()
      readerBarrier.notify(queue: .global()) {
        var localValue = 0.0;
        var value = 0.0;
        var iterations = 0
        while keepGoing.load(ordering: .relaxed) {
          lock.write { t in
            for _ in 0..<workPerCriticalSection {
              t.1 += value
              t.1 *= 1.01
              value = t.1
            }
          }

          for _ in 0..<workBetweenCriticalSections {
            localValue += value;
            localValue *= 1.01;
            value = localValue;
          }
          iterations += 1;
        }
        (iterations, value)
      }
    }

    for _ in 0..<numReaderThreads {
      writerBarrier.enter()
      writerBarrier.notify(queue: .global()) {
        var localValue = 0.0;
        var value = 0.0;
        var iterations = 0;
        while keepGoing.load(ordering: .relaxed) {
          lock.read { t in
            for _ in 0..<workPerCriticalSection {
              localValue += value;
              localValue *= t.1;
              value = localValue;
            }
          }
          for _ in 0..<workBetweenCriticalSections {
            localValue += value;
            localValue *= 1.01;
            value = localValue;
          }
          iterations += 1;
        }
        (iterations, value)
      }
    }

    for _ in 0..<numWriterThreads {
      writerBarrier.leave()
    }

    for _ in 0..<numReaderThreads {
      readerBarrier.leave()
    }
  }

  func testBenchmark() {
    let numWriterThreads = 100
    let numReaderThreads = 100
    let workPerCriticalSection = 1
    let workBetweenCriticalSections = 0
    let secondsPerTest = 1
    measure {
      self.runBenchmark(
        secondsPerTest: secondsPerTest,
        numWriterThreads: numWriterThreads,
        numReaderThreads: numReaderThreads,
        workPerCriticalSection: workPerCriticalSection,
        workBetweenCriticalSections: workBetweenCriticalSections)
    }
  }
}

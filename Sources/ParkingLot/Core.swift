protocol LockCore {
  static func lock()
  static func unlock()
  static func tryLock() -> Bool
}

protocol RWLockCore {
  associatedtype Core

  static func lockShared(_: Self.Core)
  static func unlockShared(_: Self.Core)

  static func tryLockShared(_: Self.Core) -> Bool
  static func tryLockExclusive(_: Self.Core) -> Bool

  static func unlockExclusive(_: Self.Core)
}

protocol RWFairLockCore: RWLockCore {
  static func unlockSharedFair(_: Self.Core)
  static func unlockExclusiveFair(_: Self.Core)
}



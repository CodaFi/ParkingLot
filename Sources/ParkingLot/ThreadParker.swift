import Darwin

public struct ThreadParker {
  var shouldPark: atomic_flag
  var mutex: pthread_mutex_t
  var condvar: pthread_cond_t
  var initialized: Bool

  init() {
    self.shouldPark = atomic_flag()
    self.initialized = false
    self.mutex = pthread_mutex_t()
    self.mutex.__sig = Int(_PTHREAD_MUTEX_SIG_init)
    self.condvar = pthread_cond_t()
    self.condvar.__sig = Int(_PTHREAD_COND_SIG_init)
  }
}

extension ThreadParker {
  public struct UnparkHandle {
    var rawValue: UnsafeMutablePointer<ThreadParker>

    func unpark() {
      atomic_flag_clear(self.rawValue.pointer(to: \.shouldPark)!)

      // We notify while holding the lock here to avoid races with the target
      // thread. In particular, the thread could exit after we unlock the
      // mutex, which would make the condvar access invalid memory.
      do {
        let r = pthread_cond_signal(self.rawValue.pointer(to: \.condvar)!)
        assert(r == 0)
      }

      do {
        let r = pthread_mutex_unlock(self.rawValue.pointer(to: \.mutex)!)
        assert(r == 0)
      }
    }
  }

  struct Handle {
    var rawValue: UnsafeMutablePointer<ThreadParker>

    @inline(__always)
    func preparePark() {
      atomic_flag_test_and_set(self.rawValue.pointer(to: \.shouldPark)!)
      if !self.rawValue.pointee.initialized {
        var pthreadAttr = pthread_mutexattr_t()
        do {
          let r = pthread_mutexattr_init(&pthreadAttr)
          assert(r == 0)
        }
        do {
          let r = pthread_mutexattr_settype(&pthreadAttr, PTHREAD_MUTEX_DEFAULT)
          assert(r == 0)
        }
        do {
          let r = pthread_mutex_init(self.rawValue.pointer(to: \.mutex)!, &pthreadAttr)
          assert(r == 0)
        }
        do {
          let r = pthread_mutexattr_destroy(&pthreadAttr)
          assert(r == 0)
        }

        var attr = pthread_condattr_t()
        do {
          let r = pthread_condattr_init(&attr)
          assert(r == 0)
        }
        do {
          let r = pthread_cond_init(self.rawValue.pointer(to: \.condvar)!, &attr)
          assert(r == 0)
        }
        do {
          let r = pthread_condattr_destroy(&attr)
          assert(r == 0)
        }

        self.rawValue.pointee.initialized = true
      }
    }

    @inline(__always)
    func park() {
      do {
        let r = pthread_mutex_lock(self.rawValue.pointer(to: \.mutex)!)
        assert(r == 0)
      }

      while atomic_flag_test_and_set(self.rawValue.pointer(to: \.shouldPark)!) {
        let r = pthread_cond_wait(self.rawValue.pointer(to: \.condvar)!,
                                  self.rawValue.pointer(to: \.mutex)!)
        assert(r == 0)
      }

      do {
        let r = pthread_mutex_unlock(self.rawValue.pointer(to: \.mutex)!)
        assert(r == 0)
      }
    }

    @inline(__always)
    func unparkLock() -> UnparkHandle {
      let r = pthread_mutex_lock(self.rawValue.pointer(to: \.mutex)!)
      assert(r == 0)

      return UnparkHandle(rawValue: self.rawValue)
    }
  }
}

#if defined(_MSC_VER) && _MSC_VER >= 1310 && ( defined(_M_IX86) || defined(_M_X64) )

#include <immintrin.h>

static inline void spin_loop(void) {
  _mm_pause();
}

#elif defined(__GNUC__) && ( defined(__i386__) || defined(__x86_64__) )

static inline void spin_loop(void) {
  __asm__ __volatile__ ("rep; nop" ::: "memory");
}

#elif (defined(__ARM_ARCH) && __ARM_ARCH >= 8) || defined(__ARM_ARCH_8A__) || defined(__aarch64__)

static inline void spin_loop(void) {
  __asm__ __volatile__("yield;" ::: "memory");
}

#else

#error "spin_loop not defined for this architecture"

#endif

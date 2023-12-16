#ifndef assert_always
#include <components/document/support/platform_compat.hpp>
#ifdef __cpp_exceptions
#include <stdexcept>
#else
#define _assert_failed _assert_failed_nox
#define _precondition_failed _precondition_failed_nox
#define _postcondition_failed _postcondition_failed_nox
#endif

#ifdef _MSC_VER
#define assert_always(e)                                                                                               \
    ((void) (_usually_true(!!(e)) ? ((void) 0) : document::_assert_failed(#e, __FUNCSIG__, __FILE__, __LINE__)))
#define precondition(e)                                                                                                \
    ((void) (_usually_true(!!(e)) ? ((void) 0) : ::document::_precondition_failed(#e, __FUNCSIG__, __FILE__, __LINE__)))
#define postcondition(e)                                                                                               \
    ((void) (_usually_true(!!(e)) ? ((void) 0)                                                                         \
                                  : ::document::_postcondition_failed(#e, __FUNCSIG__, __FILE__, __LINE__)))
#elif defined(__FILE_NAME__)
#define assert_always(e)                                                                                               \
    ((void) (_usually_true(!!(e)) ? ((void) 0)                                                                         \
                                  : ::document::_assert_failed(#e, __PRETTY_FUNCTION__, __FILE_NAME__, __LINE__)))
#define precondition(e)                                                                                                \
    ((void) (_usually_true(!!(e))                                                                                      \
                 ? ((void) 0)                                                                                          \
                 : ::document::_precondition_failed(#e, __PRETTY_FUNCTION__, __FILE_NAME__, __LINE__)))
#define postcondition(e)                                                                                               \
    ((void) (_usually_true(!!(e))                                                                                      \
                 ? ((void) 0)                                                                                          \
                 : ::document::_postcondition_failed(#e, __PRETTY_FUNCTION__, __FILE_NAME__, __LINE__)))
#else
#define assert_always(e)                                                                                               \
    ((void) (_usually_true(!!(e)) ? ((void) 0)                                                                         \
                                  : ::document::_assert_failed(#e, __PRETTY_FUNCTION__, __FILE__, __LINE__)))
#define precondition(e)                                                                                                \
    ((void) (_usually_true(!!(e)) ? ((void) 0)                                                                         \
                                  : ::document::_precondition_failed(#e, __PRETTY_FUNCTION__, __FILE__, __LINE__)))
#define postcondition(e)                                                                                               \
    ((void) (_usually_true(!!(e)) ? ((void) 0)                                                                         \
                                  : ::document::_postcondition_failed(#e, __PRETTY_FUNCTION__, __FILE__, __LINE__)))
#endif

namespace document {

    [[noreturn]] NOINLINE void _assert_failed(const char* condition, const char* fn, const char* file, int line);
    [[noreturn]] NOINLINE void _precondition_failed(const char* condition, const char* fn, const char* file, int line);
    [[noreturn]] NOINLINE void _postcondition_failed(const char* condition, const char* fn, const char* file, int line);

#ifdef __cpp_exceptions
    class assertion_failure : public std::logic_error {
    public:
        assertion_failure(const char* what);
    };
#endif

} // namespace document
#endif

#undef assert
#undef assert_precondition
#undef assert_postcondition
#ifdef NDEBUG
#define assert(e) (void(0))
#define assert_precondition(e) (void(0))
#define assert_postcondition(e) (void(0))
#else
#define assert(e) assert_always(e)
#define assert_precondition(e) precondition(e)
#define assert_postcondition(e) postcondition(e)
#endif

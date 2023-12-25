#include <components/cursor/cursor.hpp>
#include <cstdint>
#include <cstdlib>

#ifdef __cplusplus
extern "C" {
#endif

using namespace components::cursor;

class cursor_storage_t {
    public:
        cursor_storage_t(cursor_t_ptr cursor);

        int size();

    private:
        cursor_t_ptr cursor_;
};

#ifdef __cplusplus
}
#endif
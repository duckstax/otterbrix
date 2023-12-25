#include "otterbrix.h"
//#include "cursor_wrapper.h"

#include <integration/cpp/base_spaces.hpp>

namespace {
    configuration::config create_config() { return configuration::config::default_config(); }

    struct spaces_t final : public otterbrix::base_otterbrix_t {
    public:
        spaces_t(spaces_t& other) = delete;
        void operator=(const spaces_t&) = delete;
        spaces_t(const configuration::config& config)
            : base_otterbrix_t(config) {}
    };
} // namespace

struct pod_space_t {
    state_t state;
    std::unique_ptr<spaces_t> space;
};

extern "C" otterbrix_ptr otterbrix_create() { // (const config_t* cfg) {
    //assert(cfg != nullptr);
    auto config = create_config();
    auto pod_space = std::make_unique<pod_space_t>();
    pod_space->space = std::make_unique<spaces_t>(config);
    pod_space->state = state_t::created;
    return reinterpret_cast<void*>(pod_space.release());
}

extern "C" void otterbrix_destroy(otterbrix_ptr ptr) {
    assert(ptr != nullptr);
    auto* spaces = reinterpret_cast<pod_space_t*>(ptr);
    assert(spaces->state == state_t::created);
    spaces->space.reset();
    spaces->state = state_t::destroyed;
    delete spaces;
}

extern "C" void execute_sql(otterbrix_ptr ptr, string_view_t query_raw) {
    assert(ptr != nullptr);
    auto pod_space = reinterpret_cast<pod_space_t*>(ptr);
    assert(pod_space->state == state_t::created);
    assert(query_raw.data != nullptr);
    auto session = otterbrix::session_id_t();
    std::string query(query_raw.data, query_raw.size);
    auto cursor = pod_space->space->dispatcher()->execute_sql(session, query);
    //cursor_storage_t* cursor_storage = new cursor_storage_t(cursor);
    //return cursor_storage;
}
/*
extern "C" cursor_storage_t::cursor_storage_t(cursor_t_ptr cursor) : cursor_(cursor) {}

extern "C" int cursor_storage_t::size() {
    return static_cast<int>(cursor_->size());
}

extern "C" cursor_storage_t* RecastCursor(void* ptr) {
    return reinterpret_cast<cursor_storage_t*>(ptr);
}

extern "C" void ReleaseCursor(cursor_storage_t* storage) {
    assert(storage != nullptr);
    delete storage;
}
extern "C" int CursorSize(cursor_storage_t* storage) {
    return storage->size();
}
*/
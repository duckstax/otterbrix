#include "otterbrix.h"

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

cursor_storage_t::cursor_storage_t(components::cursor::cursor_t_ptr cursor) : cursor_(cursor) {}

int cursor_storage_t::size() {
    return static_cast<int>(cursor_->size());
}

bool cursor_storage_t::has_next() {
    return cursor_->has_next();
}

doc_ptr cursor_storage_t::next() {
    cursor_->next();
    return nullptr;
}

doc_ptr cursor_storage_t::get() {
    cursor_->get();
    return nullptr;
}

doc_ptr cursor_storage_t::get(size_t index) {
    cursor_->get(index);
    return nullptr;
}

bool cursor_storage_t::is_success() {
    return cursor_->is_success();
}

bool cursor_storage_t::is_error() {
    return cursor_->is_error();
}

error_message cursor_storage_t::get_error() {
    error_message message;
    string_view_t msg;
    auto err_msg = cursor_->get_error();

    message.error_code = static_cast<int32_t>(err_msg.type);
    msg.data = err_msg.what.data();
    msg.size = err_msg.what.size();
    message.message = msg;

    return message;
}
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

extern "C" cursor_ptr execute_sql(otterbrix_ptr ptr, string_view_t query_raw) {
    assert(ptr != nullptr);
    auto pod_space = reinterpret_cast<pod_space_t*>(ptr);
    assert(pod_space->state == state_t::created);
    assert(query_raw.data != nullptr);
    auto session = otterbrix::session_id_t();
    std::string query(query_raw.data, query_raw.size);
    auto cursor = pod_space->space->dispatcher()->execute_sql(session, query);
    cursor_storage_t* cursor_storage = new cursor_storage_t(cursor);
    return reinterpret_cast<void*>(cursor_storage);
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

extern "C" bool CursorHasNext(cursor_storage_t* storage) {
    return storage->has_next();
}

extern "C" doc_ptr CursorNext(cursor_storage_t* storage) {
    return storage->next();
}

extern "C" doc_ptr CursorGet(cursor_storage_t* storage) {
    return storage->get();
}

extern "C" doc_ptr CursorGetByIndex(cursor_storage_t* storage, int index) {
    return storage->get(static_cast<size_t>(index));
}

extern "C" bool CursorIsSuccess(cursor_storage_t* storage) {
    return storage->is_success();
}

extern "C" bool CursorIsError(cursor_storage_t* storage) {
    return storage->is_error();
}

extern "C" error_message CursorGetError(cursor_storage_t* storage) {
    return storage->get_error();
}
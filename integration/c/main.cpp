#include "otterbrix.h"

#include <integration/cpp/base_spaces.hpp>

using document_ptr = components::document::document_ptr;
using cursor_t = components::cursor::cursor_t;

namespace {
    struct spaces_t;

    struct pod_space_t {
        state_t state;
        std::unique_ptr<spaces_t> space;
    };

    struct cursor_storage_t {
        state_t state;
        boost::intrusive_ptr<cursor_t> cursor;
    };

    struct document_storage_t {
        state_t state;
        document_ptr document;
    };

    configuration::config create_config() { return configuration::config::default_config(); }

    struct spaces_t final : public otterbrix::base_otterbrix_t {
    public:
        spaces_t(spaces_t& other) = delete;
        void operator=(const spaces_t&) = delete;
        spaces_t(const configuration::config& config)
            : base_otterbrix_t(config) {}
    };

    pod_space_t* convert_otterbrix(otterbrix_ptr ptr) {
        assert(ptr != nullptr);
        auto spaces = reinterpret_cast<pod_space_t*>(ptr);
        assert(spaces->state == state_t::created);
        return spaces;
    }

    cursor_storage_t* convert_cursor(cursor_ptr ptr) {
        assert(ptr != nullptr);
        auto storage = reinterpret_cast<cursor_storage_t*>(ptr);
        assert(storage->state == state_t::created);
        return storage;
    }

    document_storage_t* convert_document(doc_ptr ptr) {
        assert(ptr != nullptr);
        auto doc_storage = reinterpret_cast<document_storage_t*>(ptr);
        assert(doc_storage->state == state_t::created);
        return doc_storage;
    }
} // namespace

extern "C" otterbrix_ptr otterbrix_create(config_t cfg) {
    auto config = create_config();
    config.log.level = static_cast<log_t::level>(cfg.level);
    config.log.path = std::pmr::string(cfg.log_path.data, cfg.log_path.size);
    config.wal.path = std::pmr::string(cfg.wal_path.data, cfg.wal_path.size);
    config.disk.path = std::pmr::string(cfg.disk_path.data, cfg.disk_path.size);
    config.main_path = std::pmr::string(cfg.main_path.data, cfg.main_path.size);
    config.wal.on = cfg.wal_on;
    config.wal.sync_to_disk = cfg.sync_to_disk;
    config.disk.on = cfg.disk_on;

    auto pod_space = std::make_unique<pod_space_t>();
    pod_space->space = std::make_unique<spaces_t>(config);
    pod_space->state = state_t::created;
    return reinterpret_cast<void*>(pod_space.release());
}

extern "C" void otterbrix_destroy(otterbrix_ptr ptr) {
    auto pod_space = convert_otterbrix(ptr);
    pod_space->space.reset();
    pod_space->state = state_t::destroyed;
    delete pod_space;
}

extern "C" cursor_ptr execute_sql(otterbrix_ptr ptr, string_view_t query_raw) {
    auto pod_space = convert_otterbrix(ptr);
    assert(query_raw.data != nullptr);
    auto session = otterbrix::session_id_t();
    std::string query(query_raw.data, query_raw.size);
    auto cursor = pod_space->space->dispatcher()->execute_sql(session, query);
    auto cursor_storage = std::make_unique<cursor_storage_t>();
    cursor_storage->cursor = cursor;
    cursor_storage->state = state_t::created;
    return reinterpret_cast<void*>(cursor_storage.release());
}

extern "C" cursor_ptr create_database(otterbrix_ptr ptr, string_view_t database_name) {
    auto pod_space = convert_otterbrix(ptr);
    assert(database_name.data != nullptr);
    auto session = otterbrix::session_id_t();
    std::string database(database_name.data, database_name.size);
    auto cursor = pod_space->space->dispatcher()->create_database(session, database);
    auto cursor_storage = std::make_unique<cursor_storage_t>();
    cursor_storage->cursor = cursor;
    cursor_storage->state = state_t::created;
    return reinterpret_cast<void*>(cursor_storage.release());
}

extern "C" cursor_ptr create_collection(otterbrix_ptr ptr, string_view_t database_name, string_view_t collection_name) {
    auto pod_space = convert_otterbrix(ptr);
    assert(database_name.data != nullptr);
    auto session = otterbrix::session_id_t();
    std::string database(database_name.data, database_name.size);
    std::string collection(collection_name.data, collection_name.size);
    auto cursor = pod_space->space->dispatcher()->create_collection(session, database, collection);
    auto cursor_storage = std::make_unique<cursor_storage_t>();
    cursor_storage->cursor = cursor;
    cursor_storage->state = state_t::created;
    return reinterpret_cast<void*>(cursor_storage.release());
}

extern "C" void release_cursor(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    storage->state = state_t::destroyed;
    delete storage;
}

extern "C" int32_t cursor_size(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    return static_cast<int32_t>(storage->cursor->size());
}

extern "C" bool cursor_has_next(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    return storage->cursor->has_next();
}

extern "C" doc_ptr cursor_next(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    auto doc_storage = std::make_unique<document_storage_t>();
    doc_storage->state = state_t::created;
    doc_storage->document = storage->cursor->next_document();
    return reinterpret_cast<void*>(doc_storage.release());
}

extern "C" doc_ptr cursor_get(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    auto doc_storage = std::make_unique<document_storage_t>();
    doc_storage->state = state_t::created;
    doc_storage->document = storage->cursor->get_document();
    return reinterpret_cast<void*>(doc_storage.release());
}

extern "C" doc_ptr cursor_get_by_index(cursor_ptr ptr, int index) {
    auto storage = convert_cursor(ptr);
    auto doc_storage = std::make_unique<document_storage_t>();
    doc_storage->state = state_t::created;
    doc_storage->document = storage->cursor->get_document(static_cast<size_t>(index));
    return reinterpret_cast<void*>(doc_storage.release());
}

extern "C" bool cursor_is_success(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    return storage->cursor->is_success();
}

extern "C" bool cursor_is_error(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    return storage->cursor->is_error();
}

extern "C" error_message cursor_get_error(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    auto error = storage->cursor->get_error();
    error_message msg;
    msg.code = static_cast<int32_t>(error.type);
    std::string str = error.what;
    msg.message = new char[sizeof(str)];
    std::strcpy(msg.message, str.data());
    return msg;
}

extern "C" void release_document(doc_ptr ptr) {
    auto doc_storage = convert_document(ptr);
    doc_storage->state = state_t::destroyed;
    delete doc_storage;
}

extern "C" char* document_id(doc_ptr ptr) {
    auto doc_storage = convert_document(ptr);
    auto str = get_document_id(doc_storage->document).to_string();
    char* str_ptr = new char[sizeof(str)];
    std::strcpy(str_ptr, str.data());
    return str_ptr;
}

extern "C" bool document_is_valid(doc_ptr ptr) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_valid();
}

extern "C" bool document_is_array(doc_ptr ptr) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_array();
}

extern "C" bool document_is_dict(doc_ptr ptr) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_dict();
}

extern "C" int32_t document_count(doc_ptr ptr) {
    auto doc_storage = convert_document(ptr);
    return static_cast<int32_t>(doc_storage->document->count());
}

extern "C" bool document_is_exist_by_key(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::pmr::string key(key_raw.data, key_raw.size);
    return doc_storage->document->is_exists(key);
}

extern "C" bool document_is_exist_by_index(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_exists(std::to_string(static_cast<uint32_t>(index)));
}

extern "C" bool document_is_null_by_key(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::pmr::string key(key_raw.data, key_raw.size);
    return doc_storage->document->is_null(key);
}

extern "C" bool document_is_null_by_index(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_null(std::to_string(static_cast<uint32_t>(index)));
}

extern "C" bool document_is_bool_by_key(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::pmr::string key(key_raw.data, key_raw.size);
    return doc_storage->document->is_bool(key);
}

extern "C" bool document_is_bool_by_index(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_bool(std::to_string(static_cast<uint32_t>(index)));
}

extern "C" bool document_is_ulong_by_key(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::pmr::string key(key_raw.data, key_raw.size);
    return doc_storage->document->is_ulong(key);
}

extern "C" bool document_is_ulong_by_index(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_ulong(std::to_string(static_cast<uint32_t>(index)));
}

extern "C" bool document_is_long_by_key(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::pmr::string key(key_raw.data, key_raw.size);
    return doc_storage->document->is_long(key);
}

extern "C" bool document_is_long_by_index(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_long(std::to_string(static_cast<uint32_t>(index)));
}

extern "C" bool document_is_double_by_key(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::pmr::string key(key_raw.data, key_raw.size);
    return doc_storage->document->is_double(key);
}

extern "C" bool document_is_double_by_index(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_double(std::to_string(static_cast<uint32_t>(index)));
}

extern "C" bool document_is_string_by_key(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::pmr::string key(key_raw.data, key_raw.size);
    return doc_storage->document->is_string(key);
}

extern "C" bool document_is_string_by_index(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_string(std::to_string(static_cast<uint32_t>(index)));
}

extern "C" bool document_is_array_by_key(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::pmr::string key(key_raw.data, key_raw.size);
    return doc_storage->document->is_array(key);
}

extern "C" bool document_is_array_by_index(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_array(std::to_string(static_cast<uint32_t>(index)));
}

extern "C" bool document_is_dict_by_key(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::pmr::string key(key_raw.data, key_raw.size);
    return doc_storage->document->is_dict(key);
}

extern "C" bool document_is_dict_by_index(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_dict(std::to_string(static_cast<uint32_t>(index)));
}

extern "C" bool document_get_bool_by_key(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::pmr::string key(key_raw.data, key_raw.size);
    return doc_storage->document->get_as<bool>(key);
}

extern "C" bool document_get_bool_by_index(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->get_as<bool>(std::to_string(static_cast<uint32_t>(index)));
}

extern "C" uint64_t document_get_ulong_by_key(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::pmr::string key(key_raw.data, key_raw.size);
    return doc_storage->document->get_as<uint64_t>(key);
}

extern "C" uint64_t document_get_ulong_by_index(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->get_as<uint64_t>(std::to_string(static_cast<uint32_t>(index)));
}

extern "C" int64_t document_get_long_by_key(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::pmr::string key(key_raw.data, key_raw.size);
    return doc_storage->document->get_as<int64_t>(key);
}

extern "C" int64_t document_get_long_by_index(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->get_as<int64_t>(std::to_string(static_cast<uint32_t>(index)));
}

extern "C" double document_get_double_by_key(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::pmr::string key(key_raw.data, key_raw.size);
    return doc_storage->document->get_as<double>(key);
}

extern "C" double document_get_double_by_index(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->get_as<double>(std::to_string(static_cast<uint32_t>(index)));
}

extern "C" char* document_get_string_by_key(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::pmr::string key(key_raw.data, key_raw.size);
    auto str = doc_storage->document->get_string(key);
    char* str_ptr = new char[sizeof(str)];
    std::strcpy(str_ptr, str.data());
    return str_ptr;
}

extern "C" char* document_get_string_by_index(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    auto str = doc_storage->document->get_string(std::to_string(static_cast<uint32_t>(index)));
    char* str_ptr = new char[sizeof(str)];
    std::strcpy(str_ptr, str.data());
    return str_ptr;
}

extern "C" doc_ptr document_get_array_by_key(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::pmr::string key(key_raw.data, key_raw.size);
    auto sub_doc_storage = std::make_unique<document_storage_t>();
    sub_doc_storage->state = state_t::created;
    sub_doc_storage->document = doc_storage->document->get_array(key);
    return reinterpret_cast<void*>(sub_doc_storage.release());
}

extern "C" doc_ptr document_get_array_by_index(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    auto sub_doc_storage = std::make_unique<document_storage_t>();
    sub_doc_storage->state = state_t::created;
    sub_doc_storage->document = doc_storage->document->get_array(std::to_string(static_cast<uint32_t>(index)));
    return reinterpret_cast<void*>(sub_doc_storage.release());
}

extern "C" doc_ptr document_get_dict_by_key(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::pmr::string key(key_raw.data, key_raw.size);
    auto sub_doc_storage = std::make_unique<document_storage_t>();
    sub_doc_storage->state = state_t::created;
    sub_doc_storage->document = doc_storage->document->get_dict(key);
    return reinterpret_cast<void*>(sub_doc_storage.release());
}

extern "C" doc_ptr document_get_dict_by_index(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    auto sub_doc_storage = std::make_unique<document_storage_t>();
    sub_doc_storage->state = state_t::created;
    sub_doc_storage->document = doc_storage->document->get_dict(std::to_string(static_cast<uint32_t>(index)));
    return reinterpret_cast<void*>(sub_doc_storage.release());
}
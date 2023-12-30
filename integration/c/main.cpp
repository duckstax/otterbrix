#include "otterbrix.h"

#include <integration/cpp/base_spaces.hpp>

struct pod_space_t;
struct cursor_storage_t;
struct document_storage_t;

namespace {
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

struct pod_space_t {
    state_t state;
    std::unique_ptr<spaces_t> space;
};

struct cursor_storage_t {
    state_t state;
    std::unique_ptr<components::cursor::cursor_t> cursor;
};

struct document_storage_t {
    state_t state;
    std::shared_ptr<const components::document::document_view_t> document;
};

extern "C" otterbrix_ptr otterbrix_create(config_t cfg, string_view_t database, string_view_t collection) {
    auto config = create_config();
    config.log.level = static_cast<log_t::level>(cfg.level);
    config.log.path = std::string(cfg.log_path.data, cfg.log_path.size);
    config.wal.path = std::string(cfg.wal_path.data, cfg.wal_path.size);
    config.disk.path = std::string(cfg.disk_path.data, cfg.disk_path.size);
    config.wal.on = cfg.wal_on;
    config.wal.sync_to_disk = cfg.sync_to_disk;
    config.disk.on = cfg.disk_on;

    auto pod_space = std::make_unique<pod_space_t>();
    pod_space->space = std::make_unique<spaces_t>(config);
    pod_space->state = state_t::created;
    auto session = otterbrix::session_id_t();
    std::string database_str(database.data, database.size);
    std::string collection_str(collection.data, collection.size);
    pod_space->space->dispatcher()->create_database(session, database_str);
    pod_space->space->dispatcher()->create_collection(session, database_str, collection_str);
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
    cursor_storage->cursor = std::unique_ptr<components::cursor::cursor_t>{cursor.get()};
    cursor.reset();
    cursor_storage->state = state_t::created;
    return reinterpret_cast<void*>(cursor_storage.release());
}

extern "C" void ReleaseCursor(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    storage->cursor.reset();
    storage->state = state_t::destroyed;
    delete storage;
}

extern "C" int32_t CursorSize(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    return static_cast<int32_t>(storage->cursor->size());
}

extern "C" bool CursorHasNext(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    return storage->cursor->has_next();
}

extern "C" doc_ptr CursorNext(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    auto doc_storage = std::make_unique<document_storage_t>();
    doc_storage->state = state_t::created;
    doc_storage->document = std::shared_ptr<const components::document::document_view_t>{storage->cursor->next()};
    return reinterpret_cast<void*>(doc_storage.release());
}

extern "C" doc_ptr CursorGet(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    auto doc_storage = std::make_unique<document_storage_t>();
    doc_storage->state = state_t::created;
    doc_storage->document = std::shared_ptr<const components::document::document_view_t>{storage->cursor->get()};
    return reinterpret_cast<void*>(doc_storage.release());
}

extern "C" doc_ptr CursorGetByIndex(cursor_ptr ptr, int index) {
    auto storage = convert_cursor(ptr);
    auto doc_storage = std::make_unique<document_storage_t>();
    doc_storage->state = state_t::created;
    doc_storage->document =
        std::shared_ptr<const components::document::document_view_t>{storage->cursor->get(static_cast<size_t>(index))};
    return reinterpret_cast<void*>(doc_storage.release());
}

extern "C" bool CursorIsSuccess(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    return storage->cursor->is_success();
}

extern "C" bool CursorIsError(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    return storage->cursor->is_error();
}

extern "C" error_message CursorGetError(cursor_ptr ptr) {
    auto storage = convert_cursor(ptr);
    auto error = storage->cursor->get_error();
    error_message msg;
    msg.code = static_cast<int>(error.type);
    std::string str = error.what;
    msg.message = new char[sizeof(str)];
    std::strcpy(msg.message, str.data());
    return msg;
}

extern "C" void ReleaseDocument(doc_ptr ptr) {
    auto doc_storage = convert_document(ptr);
    doc_storage->state = state_t::destroyed;
    delete doc_storage;
}

extern "C" char* DocumentID(doc_ptr ptr) {
    auto doc_storage = convert_document(ptr);
    auto str = doc_storage->document->id().to_string();
    char* str_ptr = new char[sizeof(str)];
    std::strcpy(str_ptr, str.data());
    return str_ptr;
}

extern "C" bool DocumentIsValid(doc_ptr ptr) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_valid();
}

extern "C" bool DocumentIsArray(doc_ptr ptr) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_array();
}

extern "C" bool DocumentIsDict(doc_ptr ptr) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_dict();
}

extern "C" int32_t DocumentCount(doc_ptr ptr) {
    auto doc_storage = convert_document(ptr);
    return static_cast<int32_t>(doc_storage->document->count());
}

extern "C" bool DocumentIsExistByKey(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::string key(key_raw.data, key_raw.size);
    return doc_storage->document->is_exists(key);
}

extern "C" bool DocumentIsExistByIndex(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_exists(static_cast<uint32_t>(index));
}

extern "C" bool DocumentIsNullByKey(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::string key(key_raw.data, key_raw.size);
    return doc_storage->document->is_null(key);
}

extern "C" bool DocumentIsNullByIndex(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_null(static_cast<uint32_t>(index));
}

extern "C" bool DocumentIsBoolByKey(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::string key(key_raw.data, key_raw.size);
    return doc_storage->document->is_bool(key);
}

extern "C" bool DocumentIsBoolByIndex(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_bool(static_cast<uint32_t>(index));
}

extern "C" bool DocumentIsUlongByKey(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::string key(key_raw.data, key_raw.size);
    return doc_storage->document->is_ulong(key);
}

extern "C" bool DocumentIsUlongByIndex(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_ulong(static_cast<uint32_t>(index));
}

extern "C" bool DocumentIsLongByKey(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::string key(key_raw.data, key_raw.size);
    return doc_storage->document->is_long(key);
}

extern "C" bool DocumentIsLongByIndex(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_long(static_cast<uint32_t>(index));
}

extern "C" bool DocumentIsDoubleByKey(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::string key(key_raw.data, key_raw.size);
    return doc_storage->document->is_double(key);
}

extern "C" bool DocumentIsDoubleByIndex(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_double(static_cast<uint32_t>(index));
}

extern "C" bool DocumentIsStringByKey(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::string key(key_raw.data, key_raw.size);
    return doc_storage->document->is_string(key);
}

extern "C" bool DocumentIsStringByIndex(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_string(static_cast<uint32_t>(index));
}

extern "C" bool DocumentIsArrayByKey(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::string key(key_raw.data, key_raw.size);
    return doc_storage->document->is_array(key);
}

extern "C" bool DocumentIsArrayByIndex(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_array(static_cast<uint32_t>(index));
}

extern "C" bool DocumentIsDictByKey(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::string key(key_raw.data, key_raw.size);
    return doc_storage->document->is_dict(key);
}

extern "C" bool DocumentIsDictByIndex(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    return doc_storage->document->is_dict(static_cast<uint32_t>(index));
}

extern "C" bool DocumentGetBoolByKey(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::string key(key_raw.data, key_raw.size);
    assert(doc_storage->document->is_bool(key));
    return doc_storage->document->get_as<bool>(key);
}

extern "C" bool DocumentGetBoolByIndex(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    assert(doc_storage->document->is_bool(index));
    return doc_storage->document->get_as<bool>(static_cast<uint32_t>(index));
}

extern "C" uint64_t DocumentGetUlongByKey(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::string key(key_raw.data, key_raw.size);
    assert(doc_storage->document->is_ulong(key));
    return doc_storage->document->get_as<uint64_t>(key);
}

extern "C" uint64_t DocumentGetUlongByIndex(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    assert(doc_storage->document->is_ulong(index));
    return doc_storage->document->get_as<uint64_t>(static_cast<uint32_t>(index));
}

extern "C" int64_t DocumentGetLongByKey(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::string key(key_raw.data, key_raw.size);
    assert(doc_storage->document->is_long(key));
    return doc_storage->document->get_as<int64_t>(key);
}

extern "C" int64_t DocumentGetLongByIndex(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    assert(doc_storage->document->is_long(index));
    return doc_storage->document->get_as<int64_t>(static_cast<uint32_t>(index));
}

extern "C" double DocumentGetDoubleByKey(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::string key(key_raw.data, key_raw.size);
    assert(doc_storage->document->is_double(key));
    return doc_storage->document->get_as<double>(key);
}

extern "C" double DocumentGetDoubleByIndex(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    assert(doc_storage->document->is_double(static_cast<uint32_t>(index)));
    return doc_storage->document->get_as<double>(static_cast<uint32_t>(index));
}

extern "C" char* DocumentGetStringByKey(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::string key(key_raw.data, key_raw.size);
    assert(doc_storage->document->is_string(key));
    auto str = doc_storage->document->get_as<std::string>(key);
    char* str_ptr = new char[sizeof(str)];
    std::strcpy(str_ptr, str.data());
    return str_ptr;
}

extern "C" char* DocumentGetStringByIndex(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    assert(doc_storage->document->is_string(static_cast<uint32_t>(index)));
    auto str = doc_storage->document->get_as<std::string>(static_cast<uint32_t>(index));
    char* str_ptr = new char[sizeof(str)];
    std::strcpy(str_ptr, str.data());
    return str_ptr;
}

extern "C" doc_ptr DocumentGetArrayByKey(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::string key(key_raw.data, key_raw.size);
    assert(doc_storage->document->is_array(key));
    auto sub_doc_storage = std::make_unique<document_storage_t>();
    sub_doc_storage->state = state_t::created;
    const components::document::document_view_t* doc =
        new components::document::document_view_t(std::move(doc_storage->document->get_array(key)));
    sub_doc_storage->document = std::shared_ptr<const components::document::document_view_t>{doc};
    return reinterpret_cast<void*>(sub_doc_storage.release());
}

extern "C" doc_ptr DocumentGetArrayByIndex(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    assert(doc_storage->document->is_array(static_cast<uint32_t>(index)));
    auto sub_doc_storage = std::make_unique<document_storage_t>();
    sub_doc_storage->state = state_t::created;
    const components::document::document_view_t* doc = new components::document::document_view_t(
        std::move(doc_storage->document->get_array(static_cast<uint32_t>(index))));
    sub_doc_storage->document = std::shared_ptr<const components::document::document_view_t>{doc};
    return reinterpret_cast<void*>(sub_doc_storage.release());
}

extern "C" doc_ptr DocumentGetDictByKey(doc_ptr ptr, string_view_t key_raw) {
    auto doc_storage = convert_document(ptr);
    std::string key(key_raw.data, key_raw.size);
    assert(doc_storage->document->is_dict(key));
    auto sub_doc_storage = std::make_unique<document_storage_t>();
    sub_doc_storage->state = state_t::created;
    const components::document::document_view_t* doc =
        new components::document::document_view_t(std::move(doc_storage->document->get_dict(key)));
    sub_doc_storage->document = std::shared_ptr<const components::document::document_view_t>{doc};
    return reinterpret_cast<void*>(sub_doc_storage.release());
}

extern "C" doc_ptr DocumentGetDictByIndex(doc_ptr ptr, int32_t index) {
    auto doc_storage = convert_document(ptr);
    assert(doc_storage->document->is_dict(static_cast<uint32_t>(index)));
    auto sub_doc_storage = std::make_unique<document_storage_t>();
    sub_doc_storage->state = state_t::created;
    const components::document::document_view_t* doc = new components::document::document_view_t(
        std::move(doc_storage->document->get_dict(static_cast<uint32_t>(index))));
    sub_doc_storage->document = std::shared_ptr<const components::document::document_view_t>{doc};
    return reinterpret_cast<void*>(sub_doc_storage.release());
}
#include "collection.hpp"
#include "database.hpp"

#include "protocol/insert.hpp"
#include "protocol/request_select.hpp"
#include "components/document/mutable/mutable_dict.h"
#include "components/document/mutable/mutable_array.h"

using ::document::impl::value_type;
using ::document::impl::mutable_dict_t;
using ::document::impl::mutable_array_t;

namespace services::storage {

collection_t::collection_t(goblin_engineer::supervisor_t* database, std::string name, log_t& log)
    : goblin_engineer::abstract_service(database, std::move(name))
    , log_(log.clone())
    , index_(mutable_dict_t::new_dict()) {
    add_handler(collection::insert_one, &collection_t::insert_one);
    add_handler(collection::insert_many, &collection_t::insert_many);
    add_handler(collection::find, &collection_t::find);
    add_handler(collection::find_one, &collection_t::find_one);
    add_handler(collection::size, &collection_t::size);
    add_handler(collection::close_cursor, &collection_t::close_cursor);
}

collection_t::~collection_t() {
    storage_.clear();
}

auto collection_t::size(session_t& session, std::string& collection) -> void {
    log_.debug("collection {}::size", collection);
    auto dispatcher = address_book("dispatcher");
    log_.debug("dispatcer : {}", dispatcher.type());
    goblin_engineer::send(dispatcher, address(), "size_finish", session, result_size(size_()));
}

void collection_t::insert_one(session_t& session, std::string& collection, document_t& document) {
    log_.debug("collection_t::insert_one : {}", collection);
    auto result = insert_(document);
    auto dispatcher = address_book("dispatcher");
    log_.debug("dispatcher : {}", dispatcher.type());
    goblin_engineer::send(dispatcher, address(), "insert_one_finish", session, result_insert_one(result));
}

void collection_t::insert_many(components::session::session_t &session, std::string &collection, std::list<document_t> &documents) {
    log_.debug("collection_t::insert_many : {}", collection);
    std::vector<std::string> result;
    for (const auto &document : documents) {
        auto id = insert_(document);
        if (!id.empty()) result.emplace_back(std::move(id));
    }
    auto dispatcher = address_book("dispatcher");
    log_.debug("dispatcher : {}", dispatcher.type());
    goblin_engineer::send(dispatcher, address(), "insert_many_finish", session, result_insert_many(std::move(result)));
}

auto collection_t::find(const session_t& session, const std::string &collection, const document_t &cond) -> void {
    log_.debug("collection::find : {}", collection);
    auto dispatcher = address_book("dispatcher");
    log_.debug("dispatcher : {}", dispatcher.type());
    auto result = cursor_storage_.emplace(session, std::make_unique<components::cursor::data_cursor_t>(*search_(parse_condition(cond))));
    goblin_engineer::send(dispatcher, address(), "find_finish", session, new components::cursor::sub_cursor_t(address(), result.first->second.get()));
}

void collection_t::find_one(const components::session::session_t &session, const std::string &collection, const document_t &cond) {
    log_.debug("collection::find_one : {}", collection);
    auto dispatcher = address_book("dispatcher");
    log_.debug("dispatcher : {}", dispatcher.type());
    goblin_engineer::send(dispatcher, address(), "find_one_finish", session, search_one_(parse_condition(cond)));
}


void collection_t::drop() {
    drop_();
}

std::string collection_t::gen_id() const {
    return std::string("0"); //todo
}

std::string collection_t::insert_(const document_t& document, int version) {
    auto id = document.is_exists("_id") ? document.get_string("_id") : gen_id();
    if (index_->get(id) == nullptr) {
        auto index = mutable_dict_t::new_dict();
        for (auto it = document.begin(); it; ++it) {
            auto key = it.key()->as_string();
            /*if (key != "_id") */index->set(key, insert_field_(it.value(), version));
        }
        index_->set(std::move(id), index);
        return id;
    }
    return std::string();
}

collection_t::field_index_t collection_t::insert_field_(collection_t::field_value_t value, int version) {
    if (value->type() == value_type::array) {
        auto index_array = mutable_array_t::new_array();
        auto array = value->as_array();
        for (uint32_t i = 0; i < array->count(); ++i) {
            index_array->append(insert_field_(array->get(i), version));
        }
        return index_array;
    }
    if (value->type() == value_type::dict) {
        auto dict = value->as_dict();
        auto index_dict = mutable_dict_t::new_dict();
        for (auto it = dict->begin(); it; ++it) {
            index_dict->set(it.key()->to_string(), insert_field_(it.value(), version));
        }
        return index_dict;
    }
    auto index = mutable_array_t::new_array();
    auto offset = storage_.size();
    msgpack::pack(storage_, document_t::get_msgpack_object(value));
    index->append(document_t::get_msgpack_type(value));
    index->append(offset);
    index->append(storage_.size() - offset);
    if (version) index->append(version);
    return index;
}

document_view_t collection_t::get_(const std::string& id) const {
    auto value = index_->get(id);
    if (value != nullptr) return document_view_t(value->as_dict(), &storage_);
    return document_view_t();
}

std::size_t collection_t::size_() const {
    return index_->count();
}

void collection_t::drop_() {
    //        storage_.clear();
}

result_find collection_t::search_(query_ptr cond) {
    result_find::result_t res;
    for (auto it = index_->begin(); it; ++it) {
        auto doc = get_(static_cast<std::string>(it.key()->as_string()));
        if (!cond || cond->check(doc)) {
            res.push_back(doc);
        }
    }
    return result_find(std::move(res));
}

result_find_one collection_t::search_one_(query_ptr cond) {
    for (auto it = index_->begin(); it; ++it) {
        auto doc = get_(static_cast<std::string>(it.key()->as_string()));
        if (!cond || cond->check(doc)) {
            return result_find_one(doc);
        }
    }
    return result_find_one();
}

auto collection_t::remove_(const std::string& key) {
    //        storage_.erase(key);
}

void collection_t::close_cursor(session_t& session) {
    cursor_storage_.erase(session);
}

#ifdef DEV_MODE
void collection_t::insert_test(document_t &&doc) {
    insert_(std::move(doc));
}

result_find collection_t::search_test(query_ptr cond) {
    return search_(std::move(cond));
}

result_find collection_t::find_test(const document_t &cond) {
    return search_(parse_condition(std::move(cond)));
}

std::string collection_t::get_index_test() const {
    return index_->to_json_string();
}

std::string collection_t::get_data_test() const {
    return std::string(storage_.data(), storage_.size());
}

std::size_t collection_t::size_test() const {
    return size_();
}

document_view_t collection_t::get_test(const std::string &id) const {
    return get_(id);
}
#endif

} // namespace services::document

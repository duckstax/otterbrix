#include "collection.hpp"
#include "database.hpp"

#include "protocol/insert.hpp"
#include "protocol/request_select.hpp"
#include "components/document/mutable/mutable_dict.h"
#include "components/document/mutable/mutable_array.h"
#include "components/document/index.hpp"

using ::document::impl::value_type;
using ::document::impl::mutable_dict_t;
using ::document::impl::mutable_array_t;

namespace services::storage {

namespace index {
    using namespace components::document::index;
}

void removed_data_t::add_range(const range_t &range) {
    range_t new_range = range;
    for (auto it = ranges_.begin(); it != ranges_.end();) {
        if (is_cross_(new_range, *it)) {
            auto it_cross = it;
            ++it;
            new_range = cross_(new_range, *it_cross);
            ranges_.erase(it_cross);
        } else {
            ++it;
        }
    }
    ranges_.push_back(new_range);
}

void removed_data_t::clear() {
    ranges_.clear();
}

bool removed_data_t::empty() const {
    return ranges_.empty();
}

void removed_data_t::sort() {
    ranges_.sort([](const range_t &r1, const range_t &r2) {
        return r1.first < r2.first;
    });
}

void removed_data_t::reverse_sort() {
    ranges_.sort([](const range_t &r1, const range_t &r2) {
        return r1.first > r2.first;
    });
}

const removed_data_t::ranges_t &removed_data_t::ranges() const {
    return ranges_;
}

template <class T>
void removed_data_t::add_document(T document) {
    for (auto it = document->begin(); it; ++it) {
        if (it.value()->as_dict()) {
            add_document(it.value()->as_dict());
        } else if (it.value()->as_array()) {
            auto a = it.value()->as_array();
            if (a->get(0)->as_array()) {
                add_document(a);
            } else {
                if (a->count() >= index::size) {
                    auto offset = a->get(index::offset)->as_unsigned();
                    auto size = a->get(index::size)->as_unsigned();
                    add_range({offset, offset + size - 1});
                }
            }
        }
    }
}

bool removed_data_t::is_cross_(const range_t &r1, const range_t r2) {
    return r1.first == r2.second + 1 || r2.first == r1.second + 1;
}

removed_data_t::range_t removed_data_t::cross_(const range_t &r1, const range_t r2) {
    return {std::min(r1.first, r2.first), std::max(r1.second, r2.second)};
}


collection_t::collection_t(goblin_engineer::supervisor_t* database, std::string name, log_t& log)
    : goblin_engineer::abstract_service(database, std::move(name))
    , log_(log.clone())
    , database_(database->address())
    , index_(mutable_dict_t::new_dict()) {
    add_handler(collection::insert_one, &collection_t::insert_one);
    add_handler(collection::insert_many, &collection_t::insert_many);
    add_handler(collection::find, &collection_t::find);
    add_handler(collection::find_one, &collection_t::find_one);
    add_handler(collection::delete_one, &collection_t::delete_one);
    add_handler(collection::delete_many, &collection_t::delete_many);
    add_handler(collection::size, &collection_t::size);
    add_handler(database::drop_collection, &collection_t::drop);
    add_handler(collection::close_cursor, &collection_t::close_cursor);
}

collection_t::~collection_t() {
    storage_.clear();
}

auto collection_t::size(session_t& session) -> void {
    log_.debug("collection {}::size", type());
    auto dispatcher = address_book("dispatcher");
    log_.debug("dispatcher : {}", dispatcher.type());
    auto result = dropped_
            ? result_size()
            : result_size(size_());
    goblin_engineer::send(dispatcher, address(), "size_finish", session, result);
}

void collection_t::insert_one(session_t& session, document_t& document) {
    log_.debug("collection_t::insert_one : {}", type());
    auto dispatcher = address_book("dispatcher");
    log_.debug("dispatcher : {}", dispatcher.type());
    auto result = dropped_
            ? result_insert_one()
            : result_insert_one(insert_(document));
    goblin_engineer::send(dispatcher, address(), "insert_one_finish", session, result);
}

void collection_t::insert_many(session_t &session, std::list<document_t> &documents) {
    log_.debug("collection_t::insert_many : {}", type());
    auto dispatcher = address_book("dispatcher");
    log_.debug("dispatcher : {}", dispatcher.type());
    if (dropped_) {
        goblin_engineer::send(dispatcher, address(), "insert_many_finish", session, result_insert_many());
    } else {
        std::vector<std::string> result;
        for (const auto &document : documents) {
            auto id = insert_(document);
            if (!id.empty()) {
                result.emplace_back(std::move(id));
            }
        }
        goblin_engineer::send(dispatcher, address(), "insert_many_finish", session, result_insert_many(std::move(result)));
    }
}

auto collection_t::find(const session_t& session, const document_t &cond) -> void {
    log_.debug("collection::find : {}", type());
    auto dispatcher = address_book("dispatcher");
    log_.debug("dispatcher : {}", dispatcher.type());
    if (dropped_) {
        goblin_engineer::send(dispatcher, address(), "find_finish", session, nullptr);
    } else {
        auto result = cursor_storage_.emplace(session, std::make_unique<components::cursor::data_cursor_t>(*search_(parse_condition(cond))));
        goblin_engineer::send(dispatcher, address(), "find_finish", session, new components::cursor::sub_cursor_t(address(), result.first->second.get()));
    }
}

void collection_t::find_one(const session_t &session, const document_t &cond) {
    log_.debug("collection::find_one : {}", type());
    auto dispatcher = address_book("dispatcher");
    log_.debug("dispatcher : {}", dispatcher.type());
    auto result = dropped_
            ? result_find_one()
            : search_one_(parse_condition(cond));
    goblin_engineer::send(dispatcher, address(), "find_one_finish", session, result);
}

auto collection_t::delete_one(const session_t &session, const document_t &cond) -> void {
    log_.debug("collection::delete_one : {}", type());
    auto dispatcher = address_book("dispatcher");
    log_.debug("dispatcher : {}", dispatcher.type());
    auto result = dropped_
            ? result_delete()
            : delete_one_(parse_condition(cond));
    goblin_engineer::send(dispatcher, address(), "delete_finish", session, result);
}

auto collection_t::delete_many(const session_t &session, const document_t &cond) -> void {
    log_.debug("collection::delete_many : {}", type());
    auto dispatcher = address_book("dispatcher");
    log_.debug("dispatcher : {}", dispatcher.type());
    auto result = dropped_
            ? result_delete()
            : delete_many_(parse_condition(cond));
    goblin_engineer::send(dispatcher, address(), "delete_finish", session, result);
}


void collection_t::drop(const session_t& session) {
    log_.debug("collection::drop : {}", type());
    auto dispatcher = address_book("dispatcher");
    log_.debug("dispatcher : {}", dispatcher.type());
    goblin_engineer::send(dispatcher, address(), "drop_collection_finish_collection", session, result_drop_collection(drop_()), std::string(database_.type()), std::string(type()));
}

std::string collection_t::gen_id() const {
    return std::string("0"); //todo
}

std::string collection_t::insert_(const document_t& document, int version) {
    auto id = document.is_exists("_id")
            ? document.get_string("_id")
            : gen_id();
    if (index_->get(id) == nullptr) {
        auto index = mutable_dict_t::new_dict();
        for (auto it = document.begin(); it; ++it) {
            auto key = it.key()->as_string();
            index->set(key, insert_field_(it.value(), version));
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
    if (version) {
        index->append(version);
    }
    return index;
}

document_view_t collection_t::get_(const std::string& id) const {
    auto value = index_->get(id);
    if (value != nullptr) {
        return document_view_t(value->as_dict(), &storage_);
    }
    return document_view_t();
}

std::size_t collection_t::size_() const {
    return index_->count();
}

bool collection_t::drop_() {
    if (dropped_) {
        return false;
    }
    dropped_ = true;
    return true;
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

result_delete collection_t::delete_one_(query_ptr cond) {
    for (auto it = index_->begin(); it; ++it) {
        auto id = static_cast<std::string>(it.key()->as_string());
        auto doc = get_(id);
        if (!cond || cond->check(doc)) {
            remove_(id);
            reindex_(); //todo
            return result_delete({id});
        }
    }
    return result_delete();
}

result_delete collection_t::delete_many_(query_ptr cond) {
    result_delete::result_t deleted;
    for (auto it = index_->begin(); it; ++it) {
        auto id = static_cast<std::string>(it.key()->as_string());
        auto doc = get_(id);
        if (!cond || cond->check(doc)) {
            deleted.push_back(id);
        }
    }
    for (const auto &id : deleted) {
        remove_(id);
    }
    reindex_(); //todo
    return result_delete(std::move(deleted));
}

result_update collection_t::update_one_(query_ptr cond, const document_t &update, bool upsert) {
    for (auto it = index_->begin(); it; ++it) {
        auto id = static_cast<std::string>(it.key()->as_string());
        auto doc = get_(id);
        if (!cond || cond->check(doc)) {
            auto res = update_(id, update)
                    ? result_update({id}, {})
                    : result_update({}, {id});
            reindex_(); //todo
            return res;
        }
    }
    if (upsert) {
        return result_update(insert_(update)); //todo convert into document by insert
    }
    return result_update();
}

result_update collection_t::update_many_(query_ptr cond, const document_t &update, bool upsert) {
    result_update::result_t modified;
    result_update::result_t nomodified;
    for (auto it = index_->begin(); it; ++it) {
        auto id = static_cast<std::string>(it.key()->as_string());
        auto doc = get_(id);
        if (!cond || cond->check(doc)) {
            if (update_(id, update)) {
                modified.push_back(id);
            } else {
                nomodified.push_back(id);
            }
        }
    }
    if (upsert && modified.empty() && nomodified.empty()) {
        return result_update(insert_(update)); //todo convert into document by insert
    }
    reindex_(); //todo
    return result_update(std::move(modified), std::move(nomodified));
}

void collection_t::remove_(const std::string &id) {
    removed_data_.add_document(index_->get(id)->as_dict());
    index_->remove(id);
}

bool collection_t::update_(const std::string &id, const document_t &update) {
    auto index = index_->get(id);
    if (index) {
        for (auto it_update = update.begin(); it_update; ++it_update) {
            auto key_update = static_cast<std::string>(it_update.key()->as_string());
            auto fields = it_update.value()->as_dict();
            for (auto it_field = fields->begin(); it_field; ++it_field) {
                auto key_field = static_cast<std::string>(it_field.key()->as_string());
                auto index_field = get_index_field(index, key_field);
                auto old_value = get_value(index_field);
                auto new_value = old_value.get();
                if (key_update == "$set") {
                    new_value = document_t::get_msgpack_object(it_field.value());
                }
                //todo others methods
                if (new_value == old_value.get()) {
                    return false;
                } else {
                    auto offset = index_field->as_array()->get(index::offset)->as_unsigned();
                    auto size = index_field->as_array()->get(index::size)->as_unsigned();
                    removed_data_.add_range({offset, offset + size - 1});
                    auto mod_index = index_field->as_array()->as_mutable();
                    auto new_offset = storage_.size();
                    msgpack::pack(storage_, new_value);
                    mod_index->set(index::offset, new_offset);
                    mod_index->set(index::size, storage_.size() - new_offset);
                    return true;
                }
            }
        }
    }
    return false;
}

collection_t::field_value_t collection_t::get_index_field(field_value_t index_doc, const std::string &field_name) const {
    std::size_t dot_pos = field_name.find(".");
    if (dot_pos != std::string::npos) {
        //todo complex keys
//        auto key = field_name.substr(0, dot_pos);
//        if (sub_doc.is_dict(key)) sub_doc = sub_doc.get_dict(key);
//        else if (sub_doc.is_array(key)) sub_doc = sub_doc.get_array(key);
//        else return false;
//        start = dot_pos + 1;
//        dot_pos = key_.find(".", start);
    } else if (index_doc->as_dict()) {
        return index_doc->as_dict()->get(field_name);
    } else if (index_doc->as_array()) {
        return index_doc->as_array()->get(static_cast<uint32_t>(std::atol(field_name.c_str())));
    }
    return nullptr;
}

msgpack::object_handle collection_t::get_value(field_value_t index) const {
    if (index && index->as_array()) {
        auto offset = index->as_array()->get(index::offset)->as_unsigned();
        auto size = index->as_array()->get(index::size)->as_unsigned();
        auto data = storage_.data() + offset;
        return msgpack::unpack(data, size);
    }
    return msgpack::object_handle();
}

void collection_t::reindex_() {
    if (removed_data_.empty()) {
        return;
    }

    //index
    removed_data_.reverse_sort();
    for (const auto &range : removed_data_.ranges()) {
        auto delta = range.second - range.first + 1;
        for (auto it = index_->begin(); it; ++it) {
            reindex_(it.value()->as_dict()->as_mutable(), range.second, delta);
        }
    }

    //buffer
    removed_data_.sort();
    std::size_t start_buffer = 0;
    storage_t buffer;
    for (const auto &range : removed_data_.ranges()) {
        if (start_buffer < range.first) {
            auto size = range.first - start_buffer;
            buffer.write(storage_.data() + start_buffer, size);
        }
        start_buffer = range.second + 1;
    }
    if (start_buffer < storage_.size()) {
        auto size = storage_.size() - start_buffer;
        buffer.write(storage_.data() + start_buffer, size);
    }
    storage_ = std::move(buffer);
    removed_data_.clear();
}

template <class T> void collection_t::reindex_(T document, std::size_t min_value, std::size_t delta) {
    for (auto it = document->begin(); it; ++it) {
        if (it.value()->as_dict()) {
            reindex_(it.value()->as_dict()->as_mutable(), min_value, delta);
        } else if (it.value()->as_array()->as_mutable()) {
            auto a = it.value()->as_array()->as_mutable();
            if (a->get(0)->as_array()) {
                reindex_(it.value()->as_array()->as_mutable(), min_value, delta);
            } else {
                if (a->count() >= index::offset) {
                    auto offset = a->get(index::offset)->as_unsigned();
                    if (offset >= min_value) {
                        a->set(index::offset, offset - delta);
                    }
                }
            }
        }
    }
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

result_delete collection_t::delete_one_test(query_ptr cond) {
    return delete_one_(std::move(cond));
}

result_delete collection_t::delete_many_test(query_ptr cond) {
    return delete_many_(std::move(cond));
}

result_update collection_t::update_one_test(query_ptr cond, const document_t &update, bool upsert) {
    return update_one_(std::move(cond), update, upsert);
}

result_update collection_t::update_many_test(query_ptr cond, const document_t &update, bool upsert) {
    return update_many_(std::move(cond), update, upsert);
}

#endif

} // namespace services::document

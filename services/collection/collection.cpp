#include "collection.hpp"

#include "components/document/index.hpp"
#include "components/document/mutable/mutable_array.h"
#include "components/document/mutable/mutable_dict.h"
#include "protocol/insert_many.hpp"

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

using ::document::impl::mutable_array_t;
using ::document::impl::mutable_dict_t;
using ::document::impl::value_type;

namespace services::storage {

    namespace index {
        using namespace components::document::index;
    }

    void removed_data_t::add_range(const range_t& range) {
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
        ranges_.sort([](const range_t& r1, const range_t& r2) {
            return r1.first < r2.first;
        });
    }

    void removed_data_t::reverse_sort() {
        ranges_.sort([](const range_t& r1, const range_t& r2) {
            return r1.first > r2.first;
        });
    }

    const removed_data_t::ranges_t& removed_data_t::ranges() const {
        return ranges_;
    }

    template<class T>
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

    bool removed_data_t::is_cross_(const range_t& r1, const range_t r2) {
        return r1.first == r2.second + 1 || r2.first == r1.second + 1;
    }

    removed_data_t::range_t removed_data_t::cross_(const range_t& r1, const range_t r2) {
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
        add_handler(collection::update_one, &collection_t::update_one);
        add_handler(collection::update_many, &collection_t::update_many);
        add_handler(collection::size, &collection_t::size);
        add_handler(collection::drop_collection, &collection_t::drop);
        add_handler(collection::close_cursor, &collection_t::close_cursor);
    }

    collection_t::~collection_t() {
        storage_.clear();
    }

    auto collection_t::size(session_id_t& session) -> void {
        log_.debug("collection {}::size", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        auto result = dropped_
                          ? result_size()
                          : result_size(size_());
        goblin_engineer::send(dispatcher, address(), "size_finish", session, result);
    }

    void collection_t::insert_one(session_id_t& session, document_t& document) {
        log_.debug("collection_t::insert_one : {}", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        auto result = dropped_
                          ? result_insert_one()
                          : result_insert_one(insert_(document));
        goblin_engineer::send(dispatcher, address(), "insert_one_finish", session, result);
    }

    void collection_t::insert_many(session_id_t& session, std::list<document_t>& documents) {
        log_.debug("collection_t::insert_many : {}", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        if (dropped_) {
            goblin_engineer::send(dispatcher, address(), "insert_many_finish", session, result_insert_many());
        } else {
            std::vector<std::string> result;
            for (const auto& document : documents) {
                auto id = insert_(document);
                if (!id.empty()) {
                    result.emplace_back(std::move(id));
                }
            }
            goblin_engineer::send(dispatcher, address(), "insert_many_finish", session, result_insert_many(std::move(result)));
        }
    }

    auto collection_t::find(const session_id_t& session, find_condition_ptr cond) -> void {
        log_.debug("collection::find : {}", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        if (dropped_) {
            goblin_engineer::send(dispatcher, address(), "find_finish", session, nullptr);
        } else {
            auto result = cursor_storage_.emplace(session, std::make_unique<components::cursor::sub_cursor_t>(address(), *search_(std::move(cond))));
            goblin_engineer::send(dispatcher, address(), "find_finish", session, result.first->second.get());
        }
    }

    void collection_t::find_one(const session_id_t& session, find_condition_ptr cond) {
        log_.debug("collection::find_one : {}", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        auto result = dropped_
                          ? result_find_one()
                          : search_one_(std::move(cond));
        goblin_engineer::send(dispatcher, address(), "find_one_finish", session, result);
    }

    auto collection_t::delete_one(const session_id_t& session, find_condition_ptr cond) -> void {
        log_.debug("collection::delete_one : {}", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        auto result = dropped_
                          ? result_delete()
                          : delete_one_(cond);
        goblin_engineer::send(dispatcher, address(), "delete_finish", session, result);
    }

    auto collection_t::delete_many(const session_id_t& session, find_condition_ptr cond) -> void {
        log_.debug("collection::delete_many : {}", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        auto result = dropped_
                          ? result_delete()
                          : delete_many_(cond);
        goblin_engineer::send(dispatcher, address(), "delete_finish", session, result);
    }

    auto collection_t::update_one(const session_id_t& session, find_condition_ptr cond, const document_t& update, bool upsert) -> void {
        log_.debug("collection::update_one : {}", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        auto result = dropped_
                          ? result_update()
                          : update_one_(cond, update, upsert);
        goblin_engineer::send(dispatcher, address(), "update_finish", session, result);
    }

    auto collection_t::update_many(const session_id_t& session, find_condition_ptr cond, const document_t& update, bool upsert) -> void {
        log_.debug("collection::update_many : {}", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        auto result = dropped_
                          ? result_update()
                          : update_many_(cond, update, upsert);
        goblin_engineer::send(dispatcher, address(), "update_finish", session, result);
    }

    void collection_t::drop(const session_id_t& session) {
        log_.debug("collection::drop : {}", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        goblin_engineer::send(dispatcher, address(), "drop_collection_finish_collection", session, result_drop_collection(drop_()), std::string(database_.type()), std::string(type()));
    }

    std::string collection_t::gen_id() const {
        //todo
        boost::uuids::random_generator generator;
        return boost::uuids::to_string(generator());
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

    result_find collection_t::search_(find_condition_ptr cond) {
        result_find::result_t res;
        for (auto it = index_->begin(); it; ++it) {
            auto doc = get_(static_cast<std::string>(it.key()->as_string()));
            if (!cond || cond->is_fit(doc)) {
                res.push_back(doc);
            }
        }
        return result_find(std::move(res));
    }

    result_find_one collection_t::search_one_(find_condition_ptr cond) {
        for (auto it = index_->begin(); it; ++it) {
            auto doc = get_(static_cast<std::string>(it.key()->as_string()));
            if (!cond || cond->is_fit(doc)) {
                return result_find_one(doc);
            }
        }
        return result_find_one();
    }

    result_delete collection_t::delete_one_(find_condition_ptr cond) {
        auto finded_doc = search_one_(std::move(cond));
        if (finded_doc.is_find()) {
            auto id = finded_doc->get_string("_id");
            remove_(id);
            reindex_();
            return result_delete({id});
        }
        return result_delete();
    }

    result_delete collection_t::delete_many_(find_condition_ptr cond) {
        result_delete::result_t deleted;
        auto finded_docs = search_(std::move(cond));
        for (auto finded_doc : *finded_docs) {
            auto id = finded_doc.get_string("_id");
            deleted.push_back(id);
        }
        for (const auto& id : deleted) {
            remove_(id);
        }
        reindex_();
        return result_delete(std::move(deleted));
    }

    result_update collection_t::update_one_(find_condition_ptr cond, const document_t& update, bool upsert) {
        auto finded_doc = search_one_(std::move(cond));
        if (finded_doc.is_find()) {
            auto id = finded_doc->get_string("_id");
            auto res = update_(id, update)
                           ? result_update({id}, {})
                           : result_update({}, {id});
            reindex_();
            return res;
        }
        if (upsert) {
            return result_update(insert_(update2insert(update)));
        }
        return result_update();
    }

    result_update collection_t::update_many_(find_condition_ptr cond, const document_t& update, bool upsert) {
        result_update::result_t modified;
        result_update::result_t nomodified;
        auto finded_docs = search_(std::move(cond));
        for (auto finded_doc : *finded_docs) {
            auto id = finded_doc.get_string("_id");
            if (update_(id, update)) {
                modified.push_back(id);
            } else {
                nomodified.push_back(id);
            }
        }
        if (upsert && modified.empty() && nomodified.empty()) {
            return result_update(insert_(update2insert(update)));
        }
        reindex_();
        return result_update(std::move(modified), std::move(nomodified));
    }

    void collection_t::remove_(const std::string& id) {
        removed_data_.add_document(index_->get(id)->as_dict());
        index_->remove(id);
    }

    bool collection_t::update_(const std::string& id, const document_t& update) {
        auto index = index_->get(id);
        bool is_modified = false;
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
                    } else if (key_update == "$inc") {
                        new_value = inc(new_value, it_field.value());
                    }
                    //todo others methods
                    if (new_value != old_value.get()) {
                        ::document::impl::mutable_array_t* mod_index = nullptr;
                        if (index_field) {
                            auto offset = index_field->as_array()->get(index::offset)->as_unsigned();
                            auto size = index_field->as_array()->get(index::size)->as_unsigned();
                            removed_data_.add_range({offset, offset + size - 1});
                            mod_index = index_field->as_array()->as_mutable();
                            auto new_offset = storage_.size();
                            msgpack::pack(storage_, new_value);
                            mod_index->set(index::offset, new_offset);
                            mod_index->set(index::size, storage_.size() - new_offset);
                        } else {
                            append_field(index, key_field, it_field.value());
                        }
                        is_modified = true;
                    }
                }
            }
        }
        return is_modified;
    }

    collection_t::field_value_t collection_t::get_index_field(field_value_t index_doc, const std::string& field_name) const {
        std::size_t dot_pos = field_name.find(".");
        if (dot_pos != std::string::npos) {
            auto parent = field_name.substr(0, dot_pos);
            auto sub_index = index_doc->type() == value_type::dict
                                 ? index_doc->as_dict()->get(parent)
                                 : index_doc->as_array()->get(static_cast<uint32_t>(std::atol(parent.c_str())));
            if (sub_index) {
                return get_index_field(sub_index, field_name.substr(dot_pos + 1, field_name.size() - dot_pos));
            }
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

    void collection_t::append_field(field_value_t index_doc, const std::string& field_name, field_value_t value) {
        std::size_t dot_pos = field_name.find(".");
        if (dot_pos != std::string::npos) {
            auto parent = field_name.substr(0, dot_pos);
            auto index_parent = index_doc->type() == value_type::dict
                                    ? index_doc->as_dict()->get(parent)
                                    : index_doc->as_array()->get(static_cast<uint32_t>(std::atol(parent.c_str())));
            if (!index_parent) {
                std::size_t dot_pos_next = field_name.find(".", dot_pos + 1);
                auto next_key = dot_pos_next != std::string::npos
                                    ? field_name.substr(dot_pos + 1, dot_pos_next - dot_pos - 1)
                                    : field_name.substr(dot_pos + 1, field_name.size() - dot_pos - 1);
                if (next_key.find_first_not_of("0123456789") == std::string::npos) {
                    index_parent = mutable_array_t::new_array().detach();
                } else {
                    index_parent = mutable_dict_t::new_dict().detach();
                }
                if (index_doc->type() == value_type::dict) {
                    index_doc->as_dict()->as_mutable()->set(parent, index_parent);
                } else if (index_doc->type() == value_type::array) {
                    index_doc->as_array()->as_mutable()->append(index_parent);
                }
            }
            append_field(index_parent, field_name.substr(dot_pos + 1, field_name.size() - dot_pos - 1), value);
        } else if (index_doc->type() == value_type::dict) {
            index_doc->as_dict()->as_mutable()->set(field_name, insert_field_(value, 0));
        } else if (index_doc->type() == value_type::array) {
            index_doc->as_array()->as_mutable()->append(insert_field_(value, 0));
        }
    }

    document_t collection_t::update2insert(const document_t& update) const {
        ::document::impl::value_t* doc = mutable_dict_t::new_dict().detach();
        for (auto it = update.begin(); it; ++it) {
            auto key = static_cast<std::string_view>(it.key()->as_string());
            if (key == "$set" || key == "$inc") {
                auto values = it.value()->as_dict();
                for (auto it_field = values->begin(); it_field; ++it_field) {
                    auto key = static_cast<std::string>(it_field.key()->as_string());
                    std::size_t dot_pos = key.find(".");
                    auto sub_doc = doc;
                    while (dot_pos != std::string::npos) {
                        auto key_parent = key.substr(0, dot_pos);
                        key = key.substr(dot_pos + 1, key.size() - dot_pos);
                        auto dot_pos_next = key.find(".");
                        auto next_key = dot_pos_next != std::string::npos
                                            ? key.substr(0, dot_pos_next - 1)
                                            : key;
                        ::document::impl::value_t* next_sub_doc = nullptr;
                        if (next_key.find_first_not_of("0123456789") == std::string::npos) {
                            next_sub_doc = mutable_array_t::new_array().detach();
                        } else {
                            next_sub_doc = mutable_dict_t::new_dict().detach();
                        }
                        if (sub_doc->type() == value_type::dict) {
                            sub_doc->as_dict()->as_mutable()->set(key_parent, next_sub_doc);
                        } else if (sub_doc->type() == value_type::array) {
                            sub_doc->as_array()->as_mutable()->append(next_sub_doc);
                        }
                        sub_doc = next_sub_doc;
                        dot_pos = dot_pos_next;
                    }
                    if (sub_doc->type() == value_type::dict) {
                        sub_doc->as_dict()->as_mutable()->set(key, it_field.value());
                    } else if (sub_doc->type() == value_type::array) {
                        sub_doc->as_array()->as_mutable()->append(it_field.value());
                    }
                }
            }
        }
        return document_t(doc->as_dict(), true);
    }

    void collection_t::reindex_() {
        if (removed_data_.empty()) {
            return;
        }

        //index
        removed_data_.reverse_sort();
        for (const auto& range : removed_data_.ranges()) {
            auto delta = range.second - range.first + 1;
            for (auto it = index_->begin(); it; ++it) {
                reindex_(it.value()->as_dict()->as_mutable(), range.second, delta);
            }
        }

        //buffer
        removed_data_.sort();
        std::size_t start_buffer = 0;
        storage_t buffer;
        for (const auto& range : removed_data_.ranges()) {
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

    msgpack::object collection_t::inc(const msgpack::object& src, const ::document::impl::value_t* value) {
        if (value->type() == ::document::impl::value_type::number) {
            if (src.type == msgpack::type::POSITIVE_INTEGER) {
                return msgpack::object(src.as<uint64_t>() + value->as_unsigned());
            }
            if (src.type == msgpack::type::NEGATIVE_INTEGER) {
                return msgpack::object(src.as<int64_t>() + value->as_int());
            }
            if (src.type == msgpack::type::FLOAT64) {
                return msgpack::object(src.as<double>() + value->as_double());
            }
        }
        return src; //todo error
    }

    template<class T>
    void collection_t::reindex_(T document, std::size_t min_value, std::size_t delta) {
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

    void collection_t::close_cursor(session_id_t& session) {
        cursor_storage_.erase(session);
    }

#ifdef DEV_MODE
    void collection_t::insert_test(document_t&& doc) {
        insert_(std::move(doc));
    }

    result_find collection_t::find_test(find_condition_ptr cond) {
        return search_(std::move(cond));
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

    document_view_t collection_t::get_test(const std::string& id) const {
        return get_(id);
    }

    result_delete collection_t::delete_one_test(find_condition_ptr cond) {
        return delete_one_(std::move(cond));
    }

    result_delete collection_t::delete_many_test(find_condition_ptr cond) {
        return delete_many_(std::move(cond));
    }

    result_update collection_t::update_one_test(find_condition_ptr cond, const document_t& update, bool upsert) {
        return update_one_(std::move(cond), update, upsert);
    }

    result_update collection_t::update_many_test(find_condition_ptr cond, const document_t& update, bool upsert) {
        return update_many_(std::move(cond), update, upsert);
    }

#endif

} // namespace services::storage

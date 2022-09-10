#include "document.hpp"

#include <components/document/mutable/value_slot.hpp>
#include <components/document/mutable/mutable_dict.hpp>
#include <components/document/mutable/mutable_dict.h>
#include <components/document/mutable/mutable_array.h>
#include <components/document/core/doc.hpp>
#include <components/document/structure.hpp>
#include <components/document/document_view.hpp>
#include <boost/json.hpp>

namespace components::document {

    using ::document::impl::mutable_array_t;
    using ::document::impl::mutable_dict_t;
    using field_index_t = ::document::retained_t<::document::impl::value_t>;
    using field_value_t = const ::document::impl::value_t*;
    using ::document::impl::value_type;


    field_index_t insert_field_(document_data_t &data, field_value_t value, int version) {
        if (value->type() == value_type::array) {
            auto index_array = mutable_array_t::new_array();
            auto array = value->as_array();
            index_array->append(value);
            for (uint32_t i = 0; i < array->count(); ++i) {
                index_array->append(insert_field_(data, array->get(i), version));
            }
            return index_array;
        }
        if (value->type() == value_type::dict) {
            auto dict = value->as_dict();
            auto index_dict = mutable_dict_t::new_dict();
            index_dict->set(key_value_document, value);
            for (auto it = dict->begin(); it; ++it) {
                index_dict->set(it.key()->to_string(), insert_field_(data, it.value(), version));
            }
            return index_dict;
        }
        auto index = mutable_array_t::new_array();
        auto offset = data.size();
        msgpack::pack(data, get_msgpack_object(value));

        index->append(get_msgpack_type(value));
        index->append(static_cast<uint64_t>(offset));
        index->append(static_cast<uint64_t>(data.size() - offset));
        index->append(value);
        if (version) {
            index->append(version);
        }
        return index;
    }

    field_value_t get_index_field_(field_value_t index_doc, const std::string& field_name) {
        std::size_t dot_pos = field_name.find('.');
        if (dot_pos != std::string::npos) {
            auto parent = field_name.substr(0, dot_pos);
            auto sub_index = index_doc->type() == value_type::dict
                                 ? index_doc->as_dict()->get(parent)
                                 : index_doc->as_array()->get(static_cast<uint32_t>(std::atol(parent.c_str())));
            if (sub_index) {
                return get_index_field_(sub_index, field_name.substr(dot_pos + 1, field_name.size() - dot_pos));
            }
        } else if (index_doc->as_dict()) {
            return index_doc->as_dict()->get(field_name);
        } else if (index_doc->as_array()) {
            return index_doc->as_array()->get(static_cast<uint32_t>(std::atol(field_name.c_str())));
        }
        return nullptr;
    }

    msgpack::object_handle get_value_(const document_data_t &data, field_value_t index) {
        if (index && index->as_array()) {
            auto offset = structure::get_attribute(index, structure::attribute::offset)->as_unsigned();
            auto size = structure::get_attribute(index, structure::attribute::size)->as_unsigned();
            auto buffer = data.data() + offset;
            return msgpack::unpack(buffer, size);
        }
        return msgpack::object_handle();
    }

    void append_field_(field_value_t index_doc, document_data_t &data, const std::string& field_name, field_value_t value, ::document::impl::value_t *doc_value) {
        std::size_t dot_pos = field_name.find('.');
        if (dot_pos != std::string::npos) {
            auto parent = field_name.substr(0, dot_pos);
            auto index_parent = index_doc->type() == value_type::dict
                                    ? index_doc->as_dict()->get(parent)
                                    : index_doc->as_array()->get(static_cast<uint32_t>(std::atol(parent.c_str())));
            if (!index_parent) {
                std::size_t dot_pos_next = field_name.find('.', dot_pos + 1);
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
            ::document::impl::value_t *sub_doc_value = nullptr;
            if (doc_value && doc_value->type() == value_type::dict) {
                sub_doc_value = doc_value->as_dict()->as_mutable()->get_mutable_dict(parent);
            }
            append_field_(index_parent, data, field_name.substr(dot_pos + 1, field_name.size() - dot_pos - 1), value, sub_doc_value);
        } else if (index_doc->type() == value_type::dict) {
            index_doc->as_dict()->as_mutable()->set(field_name, insert_field_(data, value, 0));
            if (doc_value && doc_value->type() == value_type::dict) {
                doc_value->as_dict()->as_mutable()->set(field_name, value);
            }
        } else if (index_doc->type() == value_type::array) {
            index_doc->as_array()->as_mutable()->append(insert_field_(data, value, 0));
            if (doc_value && doc_value->type() == value_type::dict) {
                doc_value->as_dict()->as_mutable()->set(field_name, value);
            }
        }
    }

    ::document::retained_const_t<::document::impl::value_t> get_value_from_msgpack_(const msgpack::object &value) {
        if (value.type == msgpack::type::object_type::BOOLEAN) return ::document::impl::new_value(value.as<bool>());
        if (value.type == msgpack::type::object_type::POSITIVE_INTEGER) return ::document::impl::new_value(value.as<uint32_t>());
        if (value.type == msgpack::type::object_type::NEGATIVE_INTEGER) return ::document::impl::new_value(value.as<long>());
        if (value.type == msgpack::type::object_type::FLOAT64) return ::document::impl::new_value(value.as<double>());
        if (value.type == msgpack::type::object_type::STR) return ::document::impl::new_value(value.as<std::string>());
        return ::document::impl::value_t::null_value;
    }

    //todo move into ...
    msgpack::object inc_(const msgpack::object& src, const ::document::impl::value_t* value) {
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
        //todo error not valid type
        return src;
    }

    template<class T>
    void reindex_(T document, std::size_t min_value, std::size_t delta) {
        for (auto it = document->begin(); it; ++it) {
            if (it.value()->as_dict()) {
                reindex_(it.value()->as_dict()->as_mutable(), min_value, delta);
            } else if (it.value()->as_array()->as_mutable()) {
                auto a = it.value()->as_array()->as_mutable();
                if (a->get(0)->as_array()) {
                    reindex_(it.value()->as_array()->as_mutable(), min_value, delta);
                } else {
                    if (structure::is_attribute(a, structure::attribute::offset)) {
                        auto offset = structure::get_attribute(a, structure::attribute::offset)->as_unsigned();
                        if (offset >= min_value) {
                            structure::set_attribute(a, structure::attribute::offset, offset - delta);
                        }
                    }
                }
            }
        }
    }


    document_t::document_t()
        : structure(mutable_dict_t::new_dict())
        , value_(nullptr) {
    }

    document_t::document_t(document_structure_t structure, const document_data_t& data)
        : structure(std::move(structure))
        , value_(nullptr) {
        this->data.write(data.data(), data.size());
    }

    bool document_t::update(const document_t& update) {
        auto dict = document_view_t(update).to_dict();
        for (auto it_update = dict->begin(); it_update; ++it_update) {
            auto key_update = static_cast<std::string>(it_update.key()->as_string());
            auto fields = it_update.value()->as_dict();
            for (auto it_field = fields->begin(); it_field; ++it_field) {
                auto key_field = static_cast<std::string>(it_field.key()->as_string());
                auto index_field = get_index_field_(structure, key_field);
                auto old_value = get_value_(data, index_field);
                auto new_value = old_value.get();
                if (key_update == "$set") {
                    new_value = get_msgpack_object(it_field.value());
                } else if (key_update == "$inc") {
                    new_value = inc_(new_value, it_field.value());
                }
                //todo impl others methods
                if (new_value != old_value.get()) {
                    ::document::impl::mutable_array_t* mod_index = nullptr;
                    if (index_field) {
                        auto offset = structure::get_attribute(index_field, structure::attribute::offset)->as_unsigned();
                        auto size = structure::get_attribute(index_field, structure::attribute::size)->as_unsigned();
                        removed_data_.add_range({offset, offset + size - 1});
                        mod_index = index_field->as_array()->as_mutable();
                        auto new_offset = data.size();
                        msgpack::pack(data, new_value);
                        structure::set_attribute(mod_index, structure::attribute::offset, static_cast<uint64_t>(new_offset));
                        structure::set_attribute(mod_index, structure::attribute::size, static_cast<uint64_t>(data.size() - new_offset));
                        if (value_) {
                            value_->as_dict()->as_mutable()->set(key_field, get_value_from_msgpack_(new_value));
                            structure::set_attribute(mod_index, structure::attribute::value, value_->as_dict()->get(key_field));
                        }
                    } else {
                        append_field_(structure, data, key_field, it_field.value(), value_);
                    }
                    return true;
                }
            }
        }
        return false;
    }

    void document_t::commit() {
        if (removed_data_.empty()) {
            return;
        }

        //structure
        removed_data_.reverse_sort();
        for (const auto& range : removed_data_.ranges()) {
            auto delta = range.second - range.first + 1;
            reindex_(structure->as_dict()->as_mutable(), range.second, delta);
        }

        //data
        removed_data_.sort();
        std::size_t start_buffer = 0;
        document_data_t buffer;
        for (const auto& range : removed_data_.ranges()) {
            if (start_buffer < range.first) {
                auto size = range.first - start_buffer;
                buffer.write(data.data() + start_buffer, size);
            }
            start_buffer = range.second + 1;
        }
        if (start_buffer < data.size()) {
            auto size = data.size() - start_buffer;
            buffer.write(data.data() + start_buffer, size);
        }
        data = std::move(buffer);
        removed_data_.clear();
        //todo impl change 0 version
    }

    void document_t::rollback() {
        //todo impl rollback
        removed_data_.clear();
    }

    void document_t::set_(const std::string &key, ::document::retained_const_t<::document::impl::value_t> value) {
        if (value_ && value_->type() == value_type::dict) {
            value_->as_dict()->as_mutable()->set(key, std::move(value));
            structure->set(key, insert_field_(data, value_->as_dict()->get(key), 0));
        } else {
            structure->set(key, insert_field_(data, value, 0));
        }
    }

    document_ptr make_document() {
        auto document = ::document::make_retained<document_t>();
        document->value_ = ::document::impl::mutable_dict_t::new_dict();
        document->structure->set(key_value_document, document->value_.get());
        return document;
    }

    document_ptr make_document(document_structure_t structure, const document_data_t &data) {
        auto document = ::document::make_retained<document_t>(std::move(structure), data);
        document->value_ = ::document::impl::mutable_dict_t::new_dict();
        document->structure->set(key_value_document, document->value_.get());
        return document;
    }

    document_ptr make_document(const ::document::impl::dict_t *dict, int version) {
        auto document = make_document();
        document->value_ = ::document::impl::mutable_dict_t::new_dict(dict);
        document->structure->set(key_value_document, document->value_.get());
        for (auto it = dict->begin(); it; ++it) {
            auto key = it.key()->as_string();
            document->structure->set(key, insert_field_(document->data, it.value(), version));
        }
        return document;
    }

    document_ptr make_upsert_document(const document_ptr& source) {
        ::document::impl::value_t* doc = mutable_dict_t::new_dict().detach();
        document_view_t view(source);
        auto update_dict = view.to_dict();
        for (auto it = update_dict->begin(); it; ++it) {
            auto cmd = static_cast<std::string_view>(it.key()->as_string());
            if (cmd == "$set" || cmd == "$inc") {
                auto values = it.value()->as_dict();
                for (auto it_field = values->begin(); it_field; ++it_field) {
                    auto key = static_cast<std::string>(it_field.key()->as_string());
                    std::size_t dot_pos = key.find('.');
                    auto sub_doc = doc;
                    while (dot_pos != std::string::npos) {
                        auto key_parent = key.substr(0, dot_pos);
                        key = key.substr(dot_pos + 1, key.size() - dot_pos);
                        auto dot_pos_next = key.find('.');
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
        doc->as_dict()->as_mutable()->set("_id", view.get_string("_id"));
        return components::document::make_document(doc->as_dict());
    }

    document_id_t get_document_id(const document_ptr &document) {
        return components::document::document_view_t(document).id();
    }

    ::document::retained_const_t<::document::impl::value_t> json2value(const boost::json::value &item) {
        if (item.is_bool()) {
            return ::document::impl::new_value(item.get_bool());
        } else if (item.is_uint64()) {
            return ::document::impl::new_value(item.get_uint64());
        } else if (item.is_int64()) {
            return ::document::impl::new_value(item.get_int64());
        } else if (item.is_double()) {
            return ::document::impl::new_value(item.get_double());
        } else if (item.is_string()) {
            return ::document::impl::new_value(::document::slice_t(item.get_string().c_str()));
        } else if (item.is_array()) {
            auto array = ::document::impl::mutable_array_t::new_array();
            for (const auto &child : item.get_array()) {
                array->append(json2value(child));
            }
            return array->as_array();
        } else if (item.is_object()) {
            auto dict = ::document::impl::mutable_dict_t::new_dict();
            for (const auto &child : item.get_object()) {
                dict->set(std::string(child.key()), json2value(child.value()));
            }
            return dict->as_dict();
        }
        return ::document::impl::value_t::null_value;
    }

    document_ptr document_from_json(const std::string &json) {
        auto doc = make_document();
        auto tree = boost::json::parse(json);
        for (const auto &item : tree.as_object()) {
            doc->set(std::string(item.key()), json2value(item.value()));
        }
        return doc;
    }

    std::string document_to_json(const document_ptr &doc) {
        return document_view_t(doc).to_json();
    }


    msgpack::type::object_type get_msgpack_type(const ::document::impl::value_t *value) {
        if (value->type() == value_type::null) return msgpack::type::NIL;
        if (value->type() == value_type::boolean) return msgpack::type::BOOLEAN;
        if (value->is_unsigned()) return msgpack::type::POSITIVE_INTEGER;
        if (value->is_int()) return msgpack::type::NEGATIVE_INTEGER;
        if (value->is_double()) return msgpack::type::FLOAT64;
        if (value->type() == value_type::string) return msgpack::type::STR;
        if (value->type() == value_type::array) return msgpack::type::ARRAY;
        if (value->type() == value_type::dict) return msgpack::type::MAP;
        return msgpack::type::NIL;
    }

    msgpack::object get_msgpack_object(const ::document::impl::value_t *value) {
        if (value->type() == value_type::boolean) return msgpack::object(value->as_bool());
        if (value->is_unsigned()) return msgpack::object(value->as_unsigned());
        if (value->is_int()) return msgpack::object(value->as_int());
        if (value->is_double()) return msgpack::object(value->as_double());
        if (value->type() == value_type::string) return msgpack::object(std::string_view(value->as_string()));
        return msgpack::object();
    }

    std::string serialize_document(const document_ptr &document) {
        return document_to_json(document);
    }

    document_ptr deserialize_document(const std::string &text) {
        return document_from_json(text);
    }

}

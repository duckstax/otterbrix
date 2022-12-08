#include "document.hpp"

#include <boost/json/src.hpp>
#include <boost/json.hpp>
#include <components/document/mutable/mutable_array.h>
#include <components/document/mutable/mutable_dict.h>
#include <components/document/document_view.hpp>

namespace components::document {

    using ::document::impl::mutable_array_t;
    using ::document::impl::mutable_dict_t;
    using ::document::impl::value_type;

    document_const_value_t inc_(document_view_t::const_value_ptr src, document_view_t::const_value_ptr value) {
        if (value->type() == ::document::impl::value_type::number) {
            if (src->is_unsigned()) {
                return ::document::impl::new_value(src->as_unsigned() + value->as_unsigned());
            } else if (src->is_int()) {
                return ::document::impl::new_value(src->as_int() + value->as_int());
            } else if (src->is_double()) {
                return ::document::impl::new_value(src->as_double() + value->as_double());
            }
        }
        //todo error not valid type
        return nullptr;
    }

    document_const_value_t get_value_by_key_(const document_const_value_t& object, const std::string &key) {
        if (object->type() == value_type::dict) {
            return object->as_dict()->as_mutable()->get(key);
        } else if (object->type() == value_type::array) {
            try {
                return object->as_array()->as_mutable()->get(uint32_t(std::atol(key.c_str())));
            } catch (...) {
            }
        }
        return nullptr;
    }

    document_const_value_t get_value_by_key_(const document_const_value_t& object, std::string_view key) {
        if (object->type() == value_type::dict) {
            return object->as_dict()->as_mutable()->get(key);
        } else if (object->type() == value_type::array) {
            try {
                return object->as_array()->as_mutable()->get(uint32_t(std::atol(key.data())));
            } catch (...) {
            }
        }
        return nullptr;
    }

    void set_new_value_(const document_const_value_t& object, const std::string &key, const document_const_value_t& value) {
        auto dot_pos = key.find('.');
        if (dot_pos != std::string::npos) {
            auto key_parent = key.substr(0, dot_pos);
            auto object_parent = get_value_by_key_(object, key_parent);
            if (!object_parent) {
                auto dot_pos_next = key.find('.', dot_pos + 1);
                auto key_next = key.substr(dot_pos + 1, (dot_pos_next == std::string::npos ? key.size() : dot_pos_next) - dot_pos - 1);
                if (key_next.find_first_not_of("0123456789") == std::string::npos) {
                    set_new_value_(object, key_parent, mutable_array_t::new_array().detach());
                } else {
                    set_new_value_(object, key_parent, mutable_dict_t::new_dict().detach());
                }
                object_parent = get_value_by_key_(object, key_parent);
                if (!object_parent && object->type() == value_type::array) {
                    object_parent = object->as_array()->get(object->as_array()->count() - 1);
                }
            }
            if (object_parent) {
                set_new_value_(object_parent, key.substr(dot_pos + 1, key.size() - dot_pos), value);
            }
        } else {
            if (object->type() == value_type::dict) {
                object->as_dict()->as_mutable()->set(key, value);
            } else if (object->type() == value_type::array) {
                try {
                    auto index = uint32_t(std::atol(key.c_str()));
                    if (index < object->as_array()->count()) {
                        object->as_array()->as_mutable()->set(index, value);
                    } else {
                        object->as_array()->as_mutable()->append(value);
                    }
                } catch (...) {
                }
            }
        }
    }

    void set_new_value_(const document_const_value_t& object, std::string_view key, const document_const_value_t& value) {
        auto dot_pos = key.find('.');
        if (dot_pos != std::string::npos) {
            auto key_parent = key.substr(0, dot_pos);
            auto object_parent = get_value_by_key_(object, key_parent);
            if (!object_parent) {
                auto dot_pos_next = key.find('.', dot_pos + 1);
                auto key_next = key.substr(dot_pos + 1, (dot_pos_next == std::string::npos ? key.size() : dot_pos_next) - dot_pos - 1);
                if (key_next.find_first_not_of("0123456789") == std::string::npos) {
                    set_new_value_(object, key_parent, mutable_array_t::new_array().detach());
                } else {
                    set_new_value_(object, key_parent, mutable_dict_t::new_dict().detach());
                }
                object_parent = get_value_by_key_(object, key_parent);
                if (!object_parent && object->type() == value_type::array) {
                    object_parent = object->as_array()->get(object->as_array()->count() - 1);
                }
            }
            if (object_parent) {
                set_new_value_(object_parent, key.substr(dot_pos + 1, key.size() - dot_pos), value);
            }
        } else {
            if (object->type() == value_type::dict) {
                object->as_dict()->as_mutable()->set(key, value);
            } else if (object->type() == value_type::array) {
                try {
                    auto index = uint32_t(std::atol(key.data()));
                    if (index < object->as_array()->count()) {
                        object->as_array()->as_mutable()->set(index, value);
                    } else {
                        object->as_array()->as_mutable()->append(value);
                    }
                } catch (...) {
                }
            }
        }
    }

    document_t::document_t()
        : value_(mutable_dict_t::new_dict()) {
    }

    document_t::document_t(document_value_t value)
        : value_(std::move(value)) {
    }

    bool document_t::update(const ptr& update) {
        bool result = false;
        document_view_t view(this);
        auto dict = document_view_t(update).as_dict();
        for (auto it_update = dict->begin(); it_update; ++it_update) {
            auto key_update = static_cast<std::string>(it_update.key()->as_string());
            auto fields = it_update.value()->as_dict();
            for (auto it_field = fields->begin(); it_field; ++it_field) {
                auto key_field = it_field.key()->as_string();
                auto old_value = view.get_value(key_field);
                document_const_value_t new_value = nullptr;
                if (key_update == "$set") {
                    new_value = ::document::impl::new_value(it_field.value());
                } else if (key_update == "$inc") {
                    new_value = inc_(old_value, it_field.value());
                }
                //todo impl others methods
                if (new_value && !new_value->is_equal(old_value)) {
                    set_(key_field, new_value);
                    result = true;
                }
            }
        }
        return result;
    }

    void document_t::set_(const std::string &key, const document_const_value_t& value) {
        set_new_value_(value_, key, value);
    }

    void document_t::set_(std::string_view key, const document_const_value_t& value) {
        set_new_value_(value_, key, value);
    }

    document_ptr make_document() {
        return new document_t();
    }

    document_ptr make_document(const ::document::impl::dict_t *dict) {
        return new document_t(::document::impl::mutable_dict_t::new_dict(dict));
    }

    document_ptr make_document(const ::document::impl::array_t *array) {
        return new document_t(::document::impl::mutable_array_t::new_array(array));
    }

    document_ptr make_document(const ::document::impl::value_t *value) {
        if (value->type() == value_type::dict) {
            return make_document(value->as_dict());
        } else if (value->type() == value_type::array) {
            return make_document(value->as_array());
        }
        return nullptr;
    }

    document_ptr make_upsert_document(const document_ptr& source) {
        auto doc = make_document();
        document_view_t view(source);
        const auto *update_dict = view.as_dict();
        for (auto it = update_dict->begin(); it; ++it) {
            auto cmd = static_cast<std::string_view>(it.key()->as_string());
            if (cmd == "$set" || cmd == "$inc") {
                auto values = it.value()->as_dict();
                for (auto it_field = values->begin(); it_field; ++it_field) {
                    auto key = static_cast<std::string>(it_field.key()->as_string());
                    doc->set(key, it_field.value());
                }
            }
        }
        doc->set("_id", view.get_string("_id"));
        return doc;
    }

    document_id_t get_document_id(const document_ptr &document) {
        return components::document::document_view_t(document).id();
    }

    bool is_equals_documents(const document_ptr& doc1, const document_ptr& doc2) {
        return document_view_t(doc1).get_value()->is_equal(document_view_t(doc2).get_value());
    }

}

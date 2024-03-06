#include "document_view.hpp"

#include <sstream>

#include <components/document/core/array.hpp>
#include <components/document/core/dict.hpp>

using ::document::impl::value_type;

namespace components::document {

    document_view_t::document_view_t()
        : document_(nullptr) {}

    document_view_t::document_view_t(document_ptr document)
        : document_(std::move(document)) {}

    document_view_t::document_view_t(document_view_t&& doc_view) noexcept
        : document_(std::move(doc_view.document_)) {
        doc_view.document_ = nullptr;
    }

    document_id_t document_view_t::id() const { return document_id_t(get_string("_id")); }

    document_ptr document_view_t::get_ptr() const { return document_; }

    bool document_view_t::is_valid() const { return document_ != nullptr; }

    bool document_view_t::is_dict() const { return document_->value_->type() == value_type::dict; }

    bool document_view_t::is_array() const { return document_->value_->type() == value_type::array; }

    std::size_t document_view_t::count() const { return is_dict() ? as_dict()->count() : as_array()->count(); }

    bool document_view_t::is_exists(const std::string& key) const { return get(key) != nullptr; }

    bool document_view_t::is_exists(std::string_view key) const { return get(key) != nullptr; }

    bool document_view_t::is_exists(uint32_t index) const { return get(index) != nullptr; }

    bool document_view_t::is_null(const std::string& key) const { return get(key)->type() == value_type::null; }

    bool document_view_t::is_null(uint32_t index) const { return get(index)->type() == value_type::null; }

    bool document_view_t::is_bool(const std::string& key) const { return get(key)->type() == value_type::boolean; }

    bool document_view_t::is_bool(uint32_t index) const { return get(index)->type() == value_type::boolean; }

    bool document_view_t::is_ulong(const std::string& key) const { return get(key)->is_unsigned(); }

    bool document_view_t::is_ulong(uint32_t index) const { return get(index)->is_unsigned(); }

    bool document_view_t::is_long(const std::string& key) const { return get(key)->is_int(); }

    bool document_view_t::is_long(uint32_t index) const { return get(index)->is_int(); }

    bool document_view_t::is_double(const std::string& key) const { return get(key)->is_double(); }

    bool document_view_t::is_double(uint32_t index) const { return get(index)->is_double(); }

    bool document_view_t::is_string(const std::string& key) const { return get(key)->type() == value_type::string; }

    bool document_view_t::is_string(uint32_t index) const { return get(index)->type() == value_type::string; }

    bool document_view_t::is_array(std::string_view key) const { return get(key)->type() == value_type::array; }

    bool document_view_t::is_array(uint32_t index) const { return get(index)->type() == value_type::array; }

    bool document_view_t::is_dict(std::string_view key) const { return get(key)->type() == value_type::dict; }

    bool document_view_t::is_dict(uint32_t index) const { return get(index)->type() == value_type::dict; }

    document_view_t::const_value_ptr document_view_t::get(const std::string& key) const {
        if (is_array()) {
            try {
                return get(uint32_t(atol(key.c_str())));
            } catch (...) {
                return nullptr;
            }
        }
        return as_dict()->get(key);
    }

    document_view_t::const_value_ptr document_view_t::get(std::string_view key) const {
        if (is_array()) {
            try {
                return get(uint32_t(atol(key.data())));
            } catch (...) {
                return nullptr;
            }
        }
        return as_dict()->get(key);
    }

    document_view_t::const_value_ptr document_view_t::get(uint32_t index) const { return as_array()->get(index); }

    bool document_view_t::get_bool(const std::string& key) const { return get_as<bool>(key); }

    uint64_t document_view_t::get_ulong(const std::string& key) const { return get_as<uint64_t>(key); }

    int64_t document_view_t::get_long(const std::string& key) const { return get_as<int64_t>(key); }

    double document_view_t::get_double(const std::string& key) const { return get_as<double>(key); }

    std::string document_view_t::get_string(const std::string& key) const { return get_as<std::string>(key); }

    document_view_t document_view_t::get_array(const std::string_view key) const {
        return document_view_t(make_document(get(key)->as_array()));
    }

    document_view_t document_view_t::get_array(uint32_t index) const {
        return document_view_t(make_document(get(index)->as_array()));
    }

    document_view_t document_view_t::get_dict(std::string_view key) const {
        return document_view_t(make_document(get(key)->as_dict()));
    }

    document_view_t document_view_t::get_dict(uint32_t index) const {
        return document_view_t(make_document(get(index)->as_dict()));
    }

    const ::document::impl::value_t* document_view_t::get_value() const { return document_->value_.get(); }

    document_view_t::const_value_ptr document_view_t::get_value(std::string_view key) const {
        auto dot_pos = key.find('.');
        if (dot_pos != std::string::npos) {
            auto key_parent = key.substr(0, dot_pos);
            if (is_exists(key_parent)) {
                auto doc_parent = make_document(get(key_parent)).detach(); //todo: memory leak
                if (doc_parent) {
                    return document_view_t(doc_parent).get_value(key.substr(dot_pos + 1, key.size() - dot_pos));
                } else {
                    return nullptr;
                }
            }
        }
        return get(key);
    }

    document_view_t::const_value_ptr document_view_t::get_value(uint32_t index) const { return get(index); }

    ::document::impl::dict_iterator_t document_view_t::begin() const { return as_dict()->begin(); }

    template<class T>
    compare_t equals_(const document_view_t& doc1, const document_view_t& doc2, const std::string& key) {
        T v1 = doc1.get_as<T>(key);
        T v2 = doc2.get_as<T>(key);
        if (v1 < v2)
            return compare_t::less;
        if (v1 > v2)
            return compare_t::more;
        return compare_t::equals;
    }

    compare_t document_view_t::compare(const document_view_t& other, const std::string& key) const {
        if (is_valid() && !other.is_valid())
            return compare_t::less;
        if (!is_valid() && other.is_valid())
            return compare_t::more;
        if (!is_valid() && !other.is_valid())
            return compare_t::equals;
        if (is_exists(key) && !other.is_exists(key))
            return compare_t::less;
        if (!is_exists(key) && other.is_exists(key))
            return compare_t::more;
        if (!is_exists(key) && !other.is_exists(key))
            return compare_t::equals;
        if (is_bool(key) && other.is_bool(key))
            return equals_<bool>(*this, other, key);
        if (is_ulong(key) && other.is_ulong(key))
            return equals_<std::uint64_t>(*this, other, key);
        if (is_long(key) && other.is_long(key))
            return equals_<int64_t>(*this, other, key);
        if (is_double(key) && other.is_double(key))
            return equals_<double>(*this, other, key);
        if (is_string(key) && other.is_string(key))
            return equals_<std::string>(*this, other, key);
        return compare_t::equals;
    }

    std::string document_view_t::to_json() const {
        if (is_dict())
            return to_json_dict();
        if (is_array())
            return to_json_array();
        return std::string();
    }
    const ::document::impl::dict_t* document_view_t::as_dict() const { return document_->value_->as_dict(); }

    const ::document::impl::array_t* document_view_t::as_array() const { return document_->value_->as_array(); }

    ::document::retained_t<::document::impl::dict_t> document_view_t::to_dict() const {
        return ::document::impl::dict_t::new_dict(as_dict());
    }

    ::document::retained_t<::document::impl::array_t> document_view_t::to_array() const {
        return ::document::impl::array_t::new_array(as_array());
    }

    std::string value_to_string(document_view_t::const_value_ptr value) {
        if (value->type() == value_type::boolean) {
            return value->as_bool() ? "true" : "false";
        } else if (value->is_unsigned()) {
            return std::to_string(value->as_unsigned());
        } else if (value->is_int()) {
            return std::to_string(value->as_int());
        } else if (value->is_double()) {
            std::stringstream res;
            res << value->as_double();
            return res.str();
        } else if (value->type() == value_type::string) {
            std::string tmp;
            tmp.append("\"").append(value->as_string()).append("\"");
            return tmp;
        }
        return std::string();
    }

    std::string document_view_t::to_json_dict() const {
        std::stringstream res;
        for (auto it = as_dict()->begin(); it; ++it) {
            auto key = it.key()->as_string();
            if (!res.str().empty())
                res << ",";
            if (is_dict(key)) {
                res << "\"" << key << "\""
                    << ":" << get_dict(key).to_json();
            } else if (is_array(key)) {
                res << "\"" << key << "\""
                    << ":" << get_array(key).to_json();
            } else {
                res << "\"" << key << "\""
                    << ":" << value_to_string(get(key));
            }
        }
        return "{" + res.str() + "}";
    }

    std::string document_view_t::to_json_array() const {
        std::stringstream res;
        for (uint32_t index = 0; index < as_array()->count(); ++index) {
            if (!res.str().empty())
                res << ",";
            if (is_dict(index)) {
                res << get_dict(index).to_json();
            } else if (is_array(index)) {
                res << get_array(index).to_json();
            } else {
                res << value_to_string(get(index));
            }
        }
        return "[" + res.str() + "]";
    }

} // namespace components::document

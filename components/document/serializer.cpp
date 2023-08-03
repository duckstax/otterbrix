#include "serializer.hpp"

#include <boost/json.hpp>

#include <components/document/core/array.hpp>
#include <components/document/core/dict.hpp>
#include <components/document/document_view.hpp>


namespace components::document {
    
    std::vector<std::uint8_t> to_msgpack(const document_ptr& j) {
        std::vector<std::uint8_t> result;
        to_msgpack(j, result);
        return result;
    }

    void to_msgpack(const document_ptr& j, output_adapter<std::uint8_t> o) {
        binary_writer<document_ptr, std::uint8_t>(o).write_msgpack(j);
    }

    void to_msgpack(const document_ptr& j, output_adapter<char> o) {
        binary_writer<document_ptr, char>(o).write_msgpack(j);
    }

    document_ptr from_msgpack(std::vector<std::uint8_t> msgpackBinaryArray) {
         return document_from_json(binary_reader(msgpackBinaryArray).msgpack_parse_to_json());
    }

    static document_const_value_t json2value(const boost::json::value &item) {
        if (item.is_bool()) {
            return ::document::impl::new_value(item.get_bool());
        } else if (item.is_uint64()) {
            return ::document::impl::new_value(item.get_uint64());
        } else if (item.is_int64()) {
            return ::document::impl::new_value(item.get_int64());
        } else if (item.is_double()) {
            return ::document::impl::new_value(item.get_double());
        } else if (item.is_string()) {
            return ::document::impl::new_value(std::string(item.get_string().c_str()));
        } else if (item.is_array()) {
            auto array = ::document::impl::array_t::new_array();
            for (const auto &child : item.get_array()) {
                array->append(json2value(child));
            }
            return array->as_array();
        } else if (item.is_object()) {
            auto dict = ::document::impl::dict_t::new_dict();
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

    std::string serialize_document(const document_ptr &document) {
        return document_to_json(document);
    }

    document_ptr deserialize_document(const std::string &text) {
        return document_from_json(text);
    }
}
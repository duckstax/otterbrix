#pragma once

#include <components/document/core/array.hpp>
#include <components/document/core/dict.hpp>
#include <components/document/document.hpp>
#include <components/document/document_view.hpp>
#include <msgpack.hpp>

using ::document::impl::array_t;
using ::document::impl::dict_t;
using ::document::impl::value_t;
using ::document::impl::value_type;

template<typename Stream>
void to_msgpack_(msgpack::packer<Stream>& o, const value_t* value) {
    if (value->type() == value_type::dict) {
        auto* dict = value->as_dict();
        o.pack_map(dict->count());
        for (auto it = dict->begin(); it; ++it) {
            o.pack(to_string(it.key()));
            to_msgpack_(o, it.value());
        }
    } else if (value->type() == value_type::array) {
        auto* array = value->as_array();
        o.pack_array(array->count());
        for (auto it = array->begin(); it; ++it) {
            to_msgpack_(o, it.value());
        }
    } else if (value->type() == value_type::boolean) {
        o.pack(value->as_bool());
    } else if (value->is_unsigned()) {
        o.pack(value->as_unsigned());
    } else if (value->is_int()) {
        o.pack(value->as_int());
    } else if (value->is_double()) {
        o.pack(value->as_double());
    } else if (value->type() == value_type::string) {
        o.pack(to_string(value));
    }
}

const value_t* to_structure_(const msgpack::object& msg_object);
void to_msgpack_(const value_t* value, msgpack::object& o);

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<components::document::document_ptr> final {
                msgpack::object const& operator()(msgpack::object const& o,
                                                  components::document::document_ptr& v) const {
                    if (o.type != msgpack::type::MAP) {
                        throw msgpack::type_error();
                    }
                    v = components::document::make_document(to_structure_(o)->as_dict());
                    return o;
                }
            };

            template<>
            struct pack<components::document::document_ptr> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o,
                                           components::document::document_ptr const& v) const {
                    to_msgpack_(o, components::document::document_view_t(v).get_value());
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::document::document_ptr> final {
                void operator()(msgpack::object::with_zone& o, components::document::document_ptr const& v) const {
                    to_msgpack_(components::document::document_view_t(v).get_value(), o);
                }
            };

        } // namespace adaptor
    } // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
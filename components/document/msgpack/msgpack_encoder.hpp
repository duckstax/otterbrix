#pragma once

#include <components/document/document.hpp>
#include <msgpack.hpp>

using components::document::document_ptr;
using components::document::document_t;
using components::document::impl::element;
using components::document::json::json_trie_node;
using components::document::json::json_type;
using components::types::logical_type;

template<typename Stream>
void to_msgpack_(msgpack::packer<Stream>& o, const element* value) {
    switch (value->logical_type()) {
        case logical_type::BOOLEAN: {
            o.pack(value->get_bool().value());
            break;
        }
        case logical_type::UTINYINT:
        case logical_type::USMALLINT:
        case logical_type::UINTEGER:
        case logical_type::UBIGINT: {
            o.pack(value->get_uint64().value());
            break;
        }
        case logical_type::TINYINT:
        case logical_type::SMALLINT:
        case logical_type::INTEGER:
        case logical_type::BIGINT: {
            o.pack(value->get_int64().value());
            break;
        }
        case logical_type::FLOAT:
        case logical_type::DOUBLE: {
            o.pack(value->get_double().value());
            break;
        }
        case logical_type::STRING_LITERAL: {
            o.pack(std::string(value->get_string().value()));
            break;
        }
        case logical_type::NA: {
            o.pack(msgpack::type::nil_t());
            break;
        }
        default:
            break;
    }
}

template<typename Stream>
void to_msgpack_(msgpack::packer<Stream>& o, const json_trie_node* value) {
    if (value->type() == json_type::OBJECT) {
        auto* dict = value->get_object();
        o.pack_map(dict->size());
        for (auto it = dict->begin(); it != dict->end(); ++it) {
            to_msgpack_(o, it->first.get());
            to_msgpack_(o, it->second.get());
        }
    } else if (value->type() == json_type::ARRAY) {
        auto* array = value->get_array();
        o.pack_array(array->size());
        for (auto it = array->begin(); it != array->end(); ++it) {
            to_msgpack_(o, it->get());
        }
    } else if (value->type() == json_type::MUT) {
        to_msgpack_(o, value->get_mut());
    }
}

void to_msgpack_(const json_trie_node* value, msgpack::object& o);

void to_msgpack_(const element* value, msgpack::object& o);

namespace components::document {
    class msgpack_decoder_t {
        msgpack_decoder_t() = delete;
        msgpack_decoder_t(const msgpack_decoder_t&) = delete;
        msgpack_decoder_t(msgpack_decoder_t&&) = delete;
        msgpack_decoder_t& operator=(const msgpack_decoder_t&) = delete;
        msgpack_decoder_t& operator=(msgpack_decoder_t&&) = delete;

    public:
        const static document_ptr to_document(const msgpack::object& msg_object);
    };
} // namespace components::document

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<document_ptr> final {
                msgpack::object const& operator()(msgpack::object const& o, document_ptr& v) const {
                    if (o.type != msgpack::type::MAP) {
                        throw msgpack::type_error();
                    }
                    v = components::document::msgpack_decoder_t::to_document(o);
                    return o;
                }
            };

            template<>
            struct pack<document_ptr> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, const document_ptr& v) const {
                    to_msgpack_(o, v->json_trie().get());
                    return o;
                }
            };

            template<>
            struct object_with_zone<document_ptr> final {
                void operator()(msgpack::object::with_zone& o, const document_ptr& v) const {
                    to_msgpack_(v->json_trie().get(), o);
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
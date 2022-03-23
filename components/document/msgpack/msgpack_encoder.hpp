#pragma once

#include <components/document/document.hpp>
#include <components/document/mutable/mutable_array.h>
#include <components/document/mutable/mutable_dict.h>
#include <msgpack.hpp>

using ::document::impl::mutable_array_t;
using ::document::impl::mutable_dict_t;
using ::document::impl::value_t;
using ::document::impl::value_type;

template<typename Stream>
void to_bin(msgpack::packer<Stream>& o, const components::document::document_ptr& doc) {
    auto data = std::make_unique<char[]>(doc->data.size());
    std::memcpy(data.get(), doc->data.data(), doc->data.size());
    o.pack_bin(doc->data.size());
    o.pack_bin_body(data.release(), doc->data.size());
}

template<typename Stream>
void to_msgpack_(msgpack::packer<Stream>& o, const value_t* structure) {
    if (structure->type() == value_type::dict) {
        auto* dict = structure->as_dict();
        o.pack_map(dict->count());
        int i = 0;
        for (auto it = dict->begin(); it; ++it) {
            //todo kick memory leak
            //msg_dict.ptr[i].key = msgpack::object(std::string_view(it.key()->to_string()));
            auto* s = new std::string(it.key()->to_string());
            o.pack(s->data());
            o.pack(it.value());
            ++i;
        }
        return;
    } else if (structure->type() == value_type::array) {
        auto* array = structure->as_array();
        o.pack_array(array->count());
        int i = 0;
        for (auto it = array->begin(); it; ++it) {
            to_msgpack_(o, it.value());
            ++i;
        }
        return;
    } else if (structure->is_unsigned()) {
        o.pack(structure->as_unsigned());
        return;
    } else if (structure->is_int()) {
        o.pack(structure->as_int());
        return;
    }
    return;
}

const value_t* to_structure_(const msgpack::object& msg_object) {
    if (msg_object.type == msgpack::type::object_type::MAP) {
        auto dict = mutable_dict_t::new_dict().detach();
        msgpack::object_map msg_dict = msg_object.via.map;
        for (uint32_t i = 0; i < msg_dict.size; ++i) {
            auto key = msg_dict.ptr[i].key.as<std::string>();
            auto val = msg_dict.ptr[i].val;
            if (val.type == msgpack::type::object_type::POSITIVE_INTEGER) {
                dict->set(key, val.as<uint64_t>());
            } else if (val.type == msgpack::type::object_type::NEGATIVE_INTEGER) {
                dict->set(key, val.as<int64_t>());
            } else {
                auto value = to_structure_(val);
                if (value) {
                    dict->set(key, value);
                }
            }
        }
        return dict;
    } else if (msg_object.type == msgpack::type::object_type::ARRAY) {
        auto array = mutable_array_t::new_array().detach();
        msgpack::object_array msg_array = msg_object.via.array;
        for (uint32_t i = 0; i < msg_array.size; ++i) {
            auto val = msg_array.ptr[i];
            if (val.type == msgpack::type::object_type::POSITIVE_INTEGER) {
                array->append(val.as<uint64_t>());
            } else if (val.type == msgpack::type::object_type::NEGATIVE_INTEGER) {
                array->append(val.as<int64_t>());
            } else {
                auto value = to_structure_(val);
                if (value) {
                    array->append(value);
                }
            }
        }
        return array;
    }
    return nullptr;
}

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct convert<components::document::document_t> final {
                msgpack::object const& operator()(msgpack::object const& o, components::document::document_ptr& v) const {
                    if (o.type != msgpack::type::ARRAY) {
                        throw msgpack::type_error();
                    }

                    msgpack::sbuffer tmp;
                    tmp.write(o.via.array.ptr[1].via.bin.ptr, o.via.array.ptr[1].via.bin.size);
                    v = components::document::make_document(to_structure_(o.via.array.ptr[0])->as_dict()->as_mutable(), tmp);
                    return o;
                }
            };

            template<>
            struct pack<components::document::document_t> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o, components::document::document_ptr const& v) const {
                    o.pack_array(2);
                    to_msgpack_(o, v->structure);
                    to_bin(o, v);
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::document::document_t> final {
                void operator()(msgpack::object::with_zone& o, components::document::document_t const& v) const {
                    o.type = type::ARRAY;
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
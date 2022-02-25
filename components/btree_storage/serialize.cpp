#include "serialize.hpp"
#include <components/document/document.hpp>
#include <components/document/mutable/mutable_dict.h>
#include <components/document/mutable/mutable_array.h>

namespace components::btree {

    using ::document::impl::value_t;
    using ::document::impl::value_type;
    using ::document::impl::mutable_array_t;
    using ::document::impl::mutable_dict_t;

    msgpack::object msgpack_object_(const msgpack::object_array &array) {
        msgpack::object res;
        res.type = msgpack::type::object_type::ARRAY;
        res.via.array = array;
        return res;
    }

    msgpack::object msgpack_object_(const msgpack::object_map &map) {
        msgpack::object res;
        res.type = msgpack::type::object_type::MAP;
        res.via.map = map;
        return res;
    }

    msgpack::object to_msgpack_(const value_t *structure) {
        if (structure->type() == value_type::dict) {
            auto* dict = structure->as_dict();
            msgpack::object_map msg_dict{dict->count(), new msgpack::object_kv[dict->count()]};
            int i = 0;
            for (auto it = dict->begin(); it; ++it) {
                //todo kick memory leak
                //msg_dict.ptr[i].key = msgpack::object(std::string_view(it.key()->to_string()));
                auto *s = new std::string(it.key()->to_string());
                msg_dict.ptr[i].key = msgpack::object(s->data());
                msg_dict.ptr[i].val = to_msgpack_(it.value());
                ++i;
            }
            return msgpack_object_(msg_dict);
        } else if (structure->type() == value_type::array) {
            auto *array = structure->as_array();
            msgpack::object_array msg_array{array->count(), new msgpack::object[array->count()]};
            int i = 0;
            for (auto it = array->begin(); it; ++it) {
                msg_array.ptr[i] = to_msgpack_(it.value());
                ++i;
            }
            return msgpack_object_(msg_array);
        } else if (structure->is_unsigned()) {
            return msgpack::object(structure->as_unsigned());
        }
        return {};
    }

    const value_t *to_structure_(const msgpack::object &msg_object) {
        if (msg_object.type == msgpack::type::object_type::MAP) {
            auto dict = mutable_dict_t::new_dict().detach();
            msgpack::object_map msg_dict = msg_object.via.map;
            for (uint32_t i = 0; i < msg_dict.size; ++i) {
                auto key = msg_dict.ptr[i].key.as<std::string>();
                auto val = msg_dict.ptr[i].val;
                if (val.type == msgpack::type::object_type::POSITIVE_INTEGER) {
                    dict->set(key, val.as<uint64_t>());
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


    msgpack::sbuffer serialize(const document_unique_ptr& document) {
        msgpack::sbuffer buffer;
        auto structure = to_msgpack_(document->structure);
        msgpack::type::tuple<std::string, msgpack::object> src(document->data.data(), structure);
        msgpack::pack(buffer, src);
        return buffer;
    }

    document_unique_ptr deserialize(const msgpack::sbuffer& buffer) {
        auto deserialized_handle = msgpack::unpack(buffer.data(), buffer.size());
        auto deserialized = deserialized_handle.get();
        auto dst = deserialized.as<msgpack::type::tuple<std::string, msgpack::object>>();
        return std::make_unique<document_t>(to_structure_(dst.get<1>())->as_dict()->as_mutable(), dst.get<0>());
    }

}

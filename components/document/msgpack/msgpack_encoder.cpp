#include "msgpack_encoder.hpp"


const value_t *to_structure(const msgpack::object &msg_object) {
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
                auto value = to_structure(val);
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
                auto value = to_structure(val);
                if (value) {
                    array->append(value);
                }
            }
        }
        return array;
    }
    return nullptr;
}
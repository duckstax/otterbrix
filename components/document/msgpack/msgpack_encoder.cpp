#include "msgpack_encoder.hpp"
#include <msgpack.hpp>


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

msgpack::object msgpack_object_(const msgpack::object_map& map, msgpack::zone& zone) {
    msgpack::object res;
    res.type = msgpack::type::object_type::MAP;
    res.via.map = map;
    return res;
}

msgpack::object msgpack_object_(const msgpack::object_array& array, msgpack::zone& zone) {
    msgpack::object res;
    res.type = msgpack::type::object_type::ARRAY;
    res.via.array = array;
    return res;
}

msgpack::object to_bin(const components::document::document_ptr& doc, msgpack::zone& zone) {
    auto data = std::make_unique<char[]>(doc->data.size());
    std::memcpy(data.get(), doc->data.data(), doc->data.size());
    msgpack::object object;
    object.via.bin = msgpack::object_bin{static_cast<uint32_t>(doc->data.size()),data.release()};
    object.type = msgpack::v1::type::BIN;
    return object;
}

msgpack::object to_msgpack_(const value_t* structure, msgpack::zone& zone) {
    if (structure->type() == value_type::dict) {
        auto* dict = structure->as_dict();
        msgpack::object_map msg_dict{dict->count(), new msgpack::object_kv[dict->count()]};
        int i = 0;
        for (auto it = dict->begin(); it; ++it) {
            //todo kick memory leak
            //msg_dict.ptr[i].key = msgpack::object(std::string_view(it.key()->to_string()));
            auto *s = new std::string(it.key()->to_string());
            msg_dict.ptr[i].key = msgpack::object(s->data(),zone);
            msg_dict.ptr[i].val = to_msgpack_(it.value(),zone);
            ++i;
        }
        return msgpack_object_(msg_dict,zone);
    } else if (structure->type() == value_type::array) {
        auto *array = structure->as_array();
        msgpack::object_array msg_array{array->count(), new msgpack::object[array->count()]};
        int i = 0;
        for (auto it = array->begin(); it; ++it) {
            msg_array.ptr[i] = to_msgpack_(it.value(),zone);
            ++i;
        }
        return msgpack_object_(msg_array,zone);
    } else if (structure->is_unsigned()) {
        return msgpack::object(structure->as_unsigned(),zone);
    } else if (structure->is_int()) {
        return msgpack::object(structure->as_int(),zone);
    }
    return {};
}
#include "msgpack_encoder.hpp"
#include <msgpack.hpp>

const value_t *to_structure_(const msgpack::object &msg_object) {
    if (msg_object.type == msgpack::type::object_type::BOOLEAN) {
        return document::impl::new_value(msg_object.as<bool>()).detach();
    } else if (msg_object.type == msgpack::type::object_type::POSITIVE_INTEGER) {
        return document::impl::new_value(msg_object.as<uint64_t>()).detach();
    } else if (msg_object.type == msgpack::type::object_type::NEGATIVE_INTEGER) {
        return document::impl::new_value(msg_object.as<int64_t>()).detach();
    } else if (msg_object.type == msgpack::type::object_type::FLOAT32 ||
               msg_object.type == msgpack::type::object_type::FLOAT64 ||
               msg_object.type == msgpack::type::object_type::FLOAT) {
        return document::impl::new_value(msg_object.as<double>()).detach();
    } else if (msg_object.type == msgpack::type::object_type::STR) {
        return document::impl::new_value(msg_object.as<std::string>()).detach();
    } else if (msg_object.type == msgpack::type::object_type::MAP) {
        auto dict = mutable_dict_t::new_dict().detach();
        msgpack::object_map msg_dict = msg_object.via.map;
        for (uint32_t i = 0; i < msg_dict.size; ++i) {
            dict->set(msg_dict.ptr[i].key.as<std::string>(), to_structure_(msg_dict.ptr[i].val));
        }
        return dict;
    } else if (msg_object.type == msgpack::type::object_type::ARRAY) {
        auto array = mutable_array_t::new_array().detach();
        msgpack::object_array msg_array = msg_object.via.array;
        for (uint32_t i = 0; i < msg_array.size; ++i) {
            array->append(to_structure_(msg_array.ptr[i]));
        }
        return array;
    }
    return nullptr;
}

void to_msgpack_(const value_t* value, msgpack::object& o) {
    if (value->type() == value_type::boolean) {
        o.type = msgpack::type::BOOLEAN;
        o.via.boolean = value->as_bool();
    } else if (value->is_unsigned()) {
        o.type = msgpack::type::POSITIVE_INTEGER;
        o.via.u64 = value->as_unsigned();
    } else if (value->is_int()) {
        o.type = msgpack::type::NEGATIVE_INTEGER;
        o.via.i64 = value->as_int();
    } else if (value->is_double()) {
        o.type = msgpack::type::FLOAT64;
        o.via.f64 = value->as_double();
    } else if (value->type() == value_type::string) {
        //todo kick memory leak
        auto *s = new std::string(to_string(value));
        o.type = msgpack::type::object_type::STR;
        o.via.str.size = uint32_t(s->size());
        o.via.str.ptr = s->c_str();
    } else if (value->type() == value_type::dict) {
        auto* dict = value->as_dict();
        o.type = msgpack::type::MAP;
        o.via.map = msgpack::object_map{dict->count(), new msgpack::object_kv[dict->count()]};
        int i = 0;
        for (auto it = dict->begin(); it; ++it) {
            to_msgpack_(it.key(), o.via.map.ptr[i].key);
            to_msgpack_(it.value(), o.via.map.ptr[i].val);
            ++i;
        }
    } else if (value->type() == value_type::array) {
        auto* array = value->as_array();
        o.type = msgpack::type::ARRAY;
        o.via.array = msgpack::object_array{array->count(), new msgpack::object[array->count()]};
        int i = 0;
        for (auto it = array->begin(); it; ++it) {
            to_msgpack_(it.value(), o.via.map.ptr[i].val);
            ++i;
        }
    }
}
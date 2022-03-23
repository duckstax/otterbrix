#include "serialize.hpp"
#include <components/document/mutable/mutable_dict.h>
#include <components/document/mutable/mutable_array.h>

namespace components::serialize {

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

    msgpack::object msgpack_object_bin(const components::document::document_ptr& doc) {
        auto data = std::make_unique<char[]>(doc->data.size());
        std::memcpy(data.get(), doc->data.data(), doc->data.size());
        msgpack::object res;
        res.type = msgpack::type::object_type::BIN;
        res.via.bin = msgpack::object_bin{static_cast<uint32_t>(doc->data.size()), data.release()};
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
        } else if (structure->is_int()) {
            return msgpack::object(structure->as_int());
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


    serialized_document_t serialize(const document_ptr& document) {
        serialized_document_t serialized_document;
        auto structure = to_msgpack_(document->structure);
        msgpack::pack(serialized_document.structure, structure);
        serialized_document.data.write(document->data.data(), document->data.size());
        return serialized_document;
    }

    document_ptr deserialize(const serialized_document_t& serialized_document) {
        auto deserialized_structure_handle = msgpack::unpack(serialized_document.structure.data(), serialized_document.structure.size());
        auto deserialized_structure = deserialized_structure_handle.get();
        auto msg_structure = deserialized_structure.as<msgpack::object>();
        return components::document::make_document(to_structure_(msg_structure)->as_dict()->as_mutable(), serialized_document.data);
    }


    msgpack::object pack(const document_ptr& document) {
        msgpack::object res;
        res.type = msgpack::type::object_type::ARRAY;
        msgpack::object_array array{2, new msgpack::object[2]};
        array.ptr[0] =to_msgpack_(document->structure);
        array.ptr[1] = msgpack_object_bin(document);
        res.via.array = array;
        return res;
    }


    document_ptr unpack(const msgpack::object& object) {
        msgpack::sbuffer tmp;
        tmp.write(object.via.array.ptr[1].via.bin.ptr,object.via.array.ptr[1].via.bin.size);

        return components::document::make_document(
            to_structure_(object.via.array.ptr[0])->as_dict()->as_mutable(),
            tmp
          );
    }

}

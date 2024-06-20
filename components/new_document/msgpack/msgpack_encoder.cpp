#include "msgpack_encoder.hpp"
#include <msgpack.hpp>

void build_primitive(
    components::new_document::tape_builder<components::new_document::impl::tape_writer_to_immutable>& builder,
    const msgpack::object& msg_object) noexcept {
    switch (msg_object.type) {
        case msgpack::type::NIL:
            builder.visit_null_atom();
            break;
        case msgpack::type::BOOLEAN:
            builder.build(msg_object.via.boolean);
            break;
        case msgpack::type::POSITIVE_INTEGER:
            builder.build(msg_object.via.u64);
            break;
        case msgpack::type::NEGATIVE_INTEGER:
            builder.build(msg_object.via.i64);
            break;
        case msgpack::type::FLOAT32:
        case msgpack::type::FLOAT64:
            builder.build(msg_object.via.f64);
            break;
        case msgpack::type::STR:
            builder.build(std::string(msg_object.via.str.ptr, msg_object.via.str.size));
            break;
    }
}

json_trie_node*
build_index(const msgpack::object& msg_object,
            components::new_document::tape_builder<components::new_document::impl::tape_writer_to_immutable>& builder,
            components::new_document::impl::immutable_document* immut_src,
            document_t::allocator_type* allocator) {
    json_trie_node* res;
    if (msg_object.type == msgpack::type::MAP) {
        res = json_trie_node::create_object(allocator);
        const auto& obj = msg_object.via.map;
        for (auto const& [current_key, val] : obj) {
            res->as_object()->set(std::string(current_key.via.str.ptr, current_key.via.str.size),
                                  build_index(val, builder, immut_src, allocator));
        }
    } else if (msg_object.type == msgpack::type::ARRAY) {
        res = json_trie_node::create_array(allocator);
        const auto& arr = msg_object.via.array;
        uint32_t i = 0;
        for (const auto& it : arr) {
            res->as_array()->set(i++, build_index(it, builder, immut_src, allocator));
        }
    } else {
        auto element = immut_src->next_element();
        build_primitive(builder, msg_object);
        res = json_trie_node::create(element, allocator);
    }
    return res;
}

const document_ptr components::new_document::msgpack_decoder_t::to_structure_(const msgpack::object& msg_object) {
    auto* allocator = std::pmr::get_default_resource();
    auto res = new (allocator->allocate(sizeof(document_t))) document_t(allocator);
    res->immut_src_ = new (allocator->allocate(sizeof(impl::immutable_document))) impl::immutable_document(allocator);

    // temp solution to size problem: convert to json and use it's size, since it is guarantied to be bigger
    // same as in document_t::document_from_json
    std::stringstream ss;
    ss << msg_object;

    if (res->immut_src_->allocate(ss.str().size()) != components::new_document::SUCCESS) {
        return nullptr;
    }
    tape_builder<impl::tape_writer_to_immutable> builder(allocator, *res->immut_src_);
    auto obj = res->element_ind_->as_object();
    for (auto& [key, val] : msg_object.via.map) {
        obj->set(std::string(key.via.str.ptr, key.via.str.size), build_index(val, builder, res->immut_src_, allocator));
    }
    return res;
}

void to_msgpack_(const json_trie_node* value, msgpack::object& o) {
    if (value->type() == json_type::OBJECT) {
        auto* dict = value->get_object();
        o.type = msgpack::type::MAP;
        o.via.map = msgpack::object_map{static_cast<uint32_t>(dict->size()), new msgpack::object_kv[dict->size()]};
        int i = 0;
        for (auto it = dict->begin(); it != dict->end(); ++it) {
            o.via.map.ptr[i].key.type = msgpack::type::object_type::STR;
            o.via.map.ptr[i].key.via.str.size = uint32_t(it->first.size());
            o.via.map.ptr[i].key.via.str.ptr = it->first.c_str();
            to_msgpack_(it->second.get(), o.via.map.ptr[i].val);
            ++i;
        }
    } else if (value->type() == json_type::ARRAY) {
        auto* array = value->get_array();
        o.type = msgpack::type::ARRAY;
        o.via.array = msgpack::object_array{static_cast<uint32_t>(array->size()), new msgpack::object[array->size()]};
        int i = 0;
        for (auto it = array->begin(); it != array->end(); ++it) {
            to_msgpack_(it->get(), o.via.map.ptr[i].val);
            ++i;
        }
    } else if (value->type() == json_type::IMMUT) {
        to_msgpack_(value->get_immut(), o);
    } else if (value->type() == json_type::MUT) {
        to_msgpack_(value->get_mut(), o);
    }
}
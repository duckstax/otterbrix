#pragma once

#include "node.hpp"
#include <components/expressions/key.hpp>

namespace components::logical_plan {

    using keys_base_storage_t = std::pmr::vector<components::expressions::key_t>;

    enum class index_type : uint8_t
    {
        single,
        composite,
        multikey,
        hashed,
        wildcard,
        no_valid = 255
    };

    class node_create_index_t final : public node_t {
    public:
        explicit node_create_index_t(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const std::string& name = "unnamed",
                                     index_type type = index_type::single);

        const std::string& name() const noexcept;
        index_type type() const noexcept;
        keys_base_storage_t& keys() noexcept;

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;

        std::string name_;
        keys_base_storage_t keys_;
        index_type index_type_;
    };

    using node_create_index_ptr = boost::intrusive_ptr<node_create_index_t>;

    node_create_index_ptr make_node_create_index(std::pmr::memory_resource* resource,
                                                 const collection_full_name_t& collection,
                                                 const std::string& name = "unnamed",
                                                 index_type type = index_type::single);

    node_create_index_ptr to_node_create_index(const msgpack::object& msg_object, std::pmr::memory_resource* resource);

} // namespace components::logical_plan

// User defined class template specialization
namespace msgpack {
    constexpr uint32_t CREATE_INDEX_SIZE = 5;
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct pack<components::logical_plan::node_create_index_ptr> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o,
                                           components::logical_plan::node_create_index_ptr const& v) const {
                    o.pack_array(CREATE_INDEX_SIZE);
                    o.pack(v->database_name());
                    o.pack(v->collection_name());
                    o.pack(v->name());
                    o.pack(static_cast<uint8_t>(v->type()));
                    o.pack(v->keys());
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::logical_plan::node_create_index_ptr> final {
                void operator()(msgpack::object::with_zone& o,
                                components::logical_plan::node_create_index_ptr const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = CREATE_INDEX_SIZE;
                    o.via.array.ptr =
                        static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size,
                                                                            MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v->database_name(), o.zone);
                    o.via.array.ptr[1] = msgpack::object(v->collection_name(), o.zone);
                    o.via.array.ptr[2] = msgpack::object(v->name(), o.zone);
                    o.via.array.ptr[3] = msgpack::object(static_cast<uint8_t>(v->type()), o.zone);
                    std::vector<std::string> tmp(v->keys().begin(), v->keys().end());
                    o.via.array.ptr[4] = msgpack::object(tmp, o.zone);
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack
#pragma once

#include "node.hpp"
#include <memory>

namespace components::logical_plan {

    class node_drop_index_t final : public node_t {
    public:
        explicit node_drop_index_t(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const std::string& name);

        const std::string& name() const noexcept;

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;

        std::string name_;
    };

    using node_drop_index_ptr = boost::intrusive_ptr<node_drop_index_t>;
    node_drop_index_ptr make_node_drop_index(std::pmr::memory_resource* resource,
                                             const collection_full_name_t& collection,
                                             const std::string& name);

    node_drop_index_ptr to_node_drop_index(const msgpack::object& msg_object, std::pmr::memory_resource* resource);

} // namespace components::logical_plan

// User defined class template specialization
namespace msgpack {
    constexpr uint32_t DROP_INDEX_SIZE = 3;
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct pack<components::logical_plan::node_drop_index_ptr> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o,
                                           components::logical_plan::node_drop_index_ptr const& v) const {
                    o.pack_array(DROP_INDEX_SIZE);
                    o.pack(v->database_name());
                    o.pack(v->collection_name());
                    o.pack(v->name());
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::logical_plan::node_drop_index_ptr> final {
                void operator()(msgpack::object::with_zone& o,
                                components::logical_plan::node_drop_index_ptr const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = DROP_INDEX_SIZE;
                    o.via.array.ptr =
                        static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size,
                                                                            MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v->database_name(), o.zone);
                    o.via.array.ptr[1] = msgpack::object(v->collection_name(), o.zone);
                    o.via.array.ptr[2] = msgpack::object(v->name(), o.zone);
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack

#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class limit_t {
        static constexpr int unlimit_ = -1;

    public:
        limit_t() = default;
        explicit limit_t(int data);

        static limit_t unlimit();
        static limit_t limit_one();

        int limit() const;
        bool check(int count) const;

    private:
        int limit_ = unlimit_;
    };

    class node_limit_t final : public node_t {
    public:
        explicit node_limit_t(std::pmr::memory_resource* resource,
                              const collection_full_name_t& collection,
                              const limit_t& limit);

        const limit_t& limit() const;

    private:
        limit_t limit_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };

    using node_limit_ptr = boost::intrusive_ptr<node_limit_t>;

    node_limit_ptr make_node_limit(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const limit_t& limit);

    node_limit_ptr to_node_limit(const msgpack::object& msg_object, std::pmr::memory_resource* resource);

} // namespace components::logical_plan

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct pack<components::logical_plan::node_limit_ptr> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o,
                                           components::logical_plan::node_limit_ptr const& v) const {
                    o.pack_array(3);
                    o.pack(v->database_name());
                    o.pack(v->collection_name());
                    o.pack(v->limit().limit());
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::logical_plan::node_limit_ptr> final {
                void operator()(msgpack::object::with_zone& o,
                                components::logical_plan::node_limit_ptr const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 3;
                    o.via.array.ptr =
                        static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size,
                                                                            MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v->database_name(), o.zone);
                    o.via.array.ptr[1] = msgpack::object(v->collection_name(), o.zone);
                    o.via.array.ptr[2] = msgpack::object(v->limit().limit(), o.zone);
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack

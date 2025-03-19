#pragma once

#include "node.hpp"

#include <components/expressions/msgpack.hpp>

namespace components::logical_plan {

    class node_match_t final : public node_t {
    public:
        explicit node_match_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };

    using node_match_ptr = boost::intrusive_ptr<node_match_t>;

    node_match_ptr make_node_match(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const expressions::expression_ptr& match);

    node_match_ptr to_node_match(const msgpack::object& msg_object, std::pmr::memory_resource* resource);

} // namespace components::logical_plan

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct pack<components::logical_plan::node_match_ptr> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o,
                                           components::logical_plan::node_match_ptr const& v) const {
                    o.pack_array(3);
                    o.pack(v->database_name());
                    o.pack(v->collection_name());
                    o.pack(v->expressions().front());
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::logical_plan::node_match_ptr> final {
                void operator()(msgpack::object::with_zone& o,
                                components::logical_plan::node_match_ptr const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 3;
                    o.via.array.ptr =
                        static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size,
                                                                            MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v->database_name(), o.zone);
                    o.via.array.ptr[1] = msgpack::object(v->collection_name(), o.zone);
                    o.via.array.ptr[2] =
                        msgpack::object(reinterpret_cast<const components::expressions::compare_expression_ptr&>(
                                            v->expressions().front()),
                                        o.zone);
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack

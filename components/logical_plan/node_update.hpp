#pragma once

#include "node.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"

#include <components/document/document.hpp>

namespace components::logical_plan {

    class node_update_t final : public node_t {
    public:
        explicit node_update_t(std::pmr::memory_resource* resource,
                               const collection_full_name_t& collection,
                               const node_match_ptr& match,
                               const node_limit_ptr& limit,
                               const components::document::document_ptr& update,
                               bool upsert = false);

        const components::document::document_ptr& update() const;
        bool upsert() const;

    private:
        components::document::document_ptr update_;
        bool upsert_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };

    using node_update_ptr = boost::intrusive_ptr<node_update_t>;

    node_update_ptr make_node_update_many(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection,
                                          const node_match_ptr& match,
                                          const components::document::document_ptr& update,
                                          bool upsert = false);

    node_update_ptr make_node_update_one(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& collection,
                                         const node_match_ptr& match,
                                         const components::document::document_ptr& update,
                                         bool upsert = false);

    node_update_ptr make_node_update(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit,
                                     const components::document::document_ptr& update,
                                     bool upsert = false);

    node_update_ptr to_node_update(const msgpack::object& msg_object, std::pmr::memory_resource* resource);

} // namespace components::logical_plan

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct pack<components::logical_plan::node_update_ptr> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o,
                                           components::logical_plan::node_update_ptr const& v) const {
                    o.pack_array(6);
                    o.pack(v->database_name());
                    o.pack(v->collection_name());
                    o.pack(reinterpret_cast<const components::logical_plan::node_match_ptr&>(v->children().at(0)));
                    o.pack(reinterpret_cast<const components::logical_plan::node_limit_ptr&>(v->children().at(1)));
                    o.pack(v->update());
                    o.pack(v->upsert());
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::logical_plan::node_update_ptr> final {
                void operator()(msgpack::object::with_zone& o,
                                components::logical_plan::node_update_ptr const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 6;
                    o.via.array.ptr =
                        static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size,
                                                                            MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v->database_name(), o.zone);
                    o.via.array.ptr[1] = msgpack::object(v->collection_name(), o.zone);
                    o.via.array.ptr[2] = msgpack::object(
                        reinterpret_cast<const components::logical_plan::node_match_ptr&>(v->children().at(0)),
                        o.zone);
                    o.via.array.ptr[3] = msgpack::object(
                        reinterpret_cast<const components::logical_plan::node_limit_ptr&>(v->children().at(1)),
                        o.zone);
                    o.via.array.ptr[4] = msgpack::object(v->update(), o.zone);
                    o.via.array.ptr[5] = msgpack::object(v->upsert(), o.zone);
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack

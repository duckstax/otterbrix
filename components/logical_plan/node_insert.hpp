#pragma once

#include "node.hpp"

#include <components/document/document.hpp>
#include <components/document/msgpack/msgpack_encoder.hpp>

namespace components::logical_plan {

    class node_insert_t final : public node_t {
    public:
        explicit node_insert_t(std::pmr::memory_resource* resource,
                               const collection_full_name_t& collection,
                               const std::pmr::vector<components::document::document_ptr>& documents);

        explicit node_insert_t(std::pmr::memory_resource* resource,
                               const collection_full_name_t& collection,
                               std::pmr::vector<components::document::document_ptr>&& documents);

        const std::pmr::vector<components::document::document_ptr>& documents() const;

    private:
        std::pmr::vector<components::document::document_ptr> documents_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };

    using node_insert_ptr = boost::intrusive_ptr<node_insert_t>;
    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const std::pmr::vector<components::document::document_ptr>& documents);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     std::pmr::vector<components::document::document_ptr>&& documents);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     components::document::document_ptr document);

    inline node_insert_ptr to_node_insert(const msgpack::object& msg_object, std::pmr::memory_resource* resource) {
        if (msg_object.type != msgpack::type::ARRAY) {
            throw msgpack::type_error();
        }
        if (msg_object.via.array.size != 3) {
            throw msgpack::type_error();
        }
        auto database = msg_object.via.array.ptr[0].as<std::string>();
        auto collection = msg_object.via.array.ptr[1].as<std::string>();
        auto documents = document::to_documents(msg_object.via.array.ptr[2], resource);
        return make_node_insert(resource, {database, collection}, std::move(documents));
    }

} // namespace components::logical_plan

// User defined class template specialization
namespace msgpack {
    MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS) {
        namespace adaptor {

            template<>
            struct pack<components::logical_plan::node_insert_ptr> final {
                template<typename Stream>
                packer<Stream>& operator()(msgpack::packer<Stream>& o,
                                           components::logical_plan::node_insert_ptr const& v) const {
                    o.pack_array(3);
                    o.pack(v->database_name());
                    o.pack(v->collection_name());
                    o.pack(v->documents());
                    return o;
                }
            };

            template<>
            struct object_with_zone<components::logical_plan::node_insert_ptr> final {
                void operator()(msgpack::object::with_zone& o,
                                components::logical_plan::node_insert_ptr const& v) const {
                    o.type = type::ARRAY;
                    o.via.array.size = 3;
                    o.via.array.ptr =
                        static_cast<msgpack::object*>(o.zone.allocate_align(sizeof(msgpack::object) * o.via.array.size,
                                                                            MSGPACK_ZONE_ALIGNOF(msgpack::object)));
                    o.via.array.ptr[0] = msgpack::object(v->database_name(), o.zone);
                    o.via.array.ptr[1] = msgpack::object(v->collection_name(), o.zone);
                    o.via.array.ptr[2] = msgpack::object(v->documents(), o.zone);
                }
            };

        } // namespace adaptor
    }     // MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
} // namespace msgpack

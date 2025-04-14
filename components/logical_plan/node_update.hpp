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

        static node_ptr deserialize(serializer::base_deserializer_t* deserializer);

    private:
        components::document::document_ptr update_;
        bool upsert_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        void serialize_impl(serializer::base_serializer_t* serializer) const final;
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

} // namespace components::logical_plan
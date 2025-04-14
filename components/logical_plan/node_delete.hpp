#pragma once

#include "node.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"

namespace components::logical_plan {

    class node_delete_t final : public node_t {
    public:
        explicit node_delete_t(std::pmr::memory_resource* resource,
                               const collection_full_name_t& collection,
                               const node_match_ptr& match,
                               const node_limit_ptr& limit);

        static node_ptr deserialize(serializer::base_deserializer_t* deserializer);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        void serialize_impl(serializer::base_serializer_t* serializer) const final;
    };

    using node_delete_ptr = boost::intrusive_ptr<node_delete_t>;

    node_delete_ptr make_node_delete_many(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection,
                                          const node_match_ptr& match);

    node_delete_ptr make_node_delete_one(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& collection,
                                         const node_match_ptr& match);

    node_delete_ptr make_node_delete(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit);

} // namespace components::logical_plan
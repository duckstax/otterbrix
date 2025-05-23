#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_drop_database_t final : public node_t {
    public:
        explicit node_drop_database_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

        static node_ptr deserialize(serializer::base_deserializer_t* deserializer);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        void serialize_impl(serializer::base_serializer_t* serializer) const final;
    };

    using node_drop_database_ptr = boost::intrusive_ptr<node_drop_database_t>;
    node_drop_database_ptr make_node_drop_database(std::pmr::memory_resource* resource,
                                                   const collection_full_name_t& collection);

} // namespace components::logical_plan

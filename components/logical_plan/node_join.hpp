#pragma once

#include "node.hpp"

namespace components::logical_plan {

    enum class join_type : uint8_t
    {
        inner,
        full,
        left,
        right,
        cross
    };

    class node_join_t final : public node_t {
    public:
        explicit node_join_t(std::pmr::memory_resource* resource,
                             const collection_full_name_t& collection,
                             join_type type);

        join_type type() const;

        static node_ptr deserialize(serializer::base_deserializer_t* deserializer);

    private:
        join_type type_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        void serialize_impl(serializer::base_serializer_t* serializer) const final;
    };

    using node_join_ptr = boost::intrusive_ptr<node_join_t>;

    node_join_ptr
    make_node_join(std::pmr::memory_resource* resource, const collection_full_name_t& collection, join_type type);

} // namespace components::logical_plan

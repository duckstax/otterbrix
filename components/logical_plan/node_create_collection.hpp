#pragma once

#include "node.hpp"

#include <components/types/types.hpp>

namespace components::logical_plan {

    class node_create_collection_t final : public node_t {
    public:
        explicit node_create_collection_t(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection,
                                          const types::complex_logical_type& schema = {});

        static node_ptr deserialize(serializer::base_deserializer_t* deserializer);

        types::complex_logical_type schema() const;

    private:
        types::complex_logical_type schema_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        void serialize_impl(serializer::base_serializer_t* serializer) const final;
    };

    using node_create_collection_ptr = boost::intrusive_ptr<node_create_collection_t>;
    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           const collection_full_name_t& collection,
                                                           const types::complex_logical_type& schema = {});

} // namespace components::logical_plan

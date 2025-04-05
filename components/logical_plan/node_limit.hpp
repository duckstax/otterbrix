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

        static node_ptr deserialize(serializer::base_deserializer_t* deserializer);

    private:
        limit_t limit_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        void serialize_impl(serializer::base_serializer_t* serializer) const final;
    };

    using node_limit_ptr = boost::intrusive_ptr<node_limit_t>;

    node_limit_ptr make_node_limit(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const limit_t& limit);

} // namespace components::logical_plan
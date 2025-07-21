#pragma once

#include "catalog_exception.hpp"
#include "catalog_types.hpp"
#include "table_id.hpp"
#include "table_metadata.hpp"

#include "trie/trie_boost.hpp"
//#include <boost/parser/detail/text/trie_map.hpp>

namespace components::catalog {
    class namespace_storage {
        struct namespace_info;

    public:
        explicit namespace_storage(std::pmr::memory_resource* resource);

        void create_namespace(const table_namespace_t& namespace_name);
        void drop_namespace(const table_namespace_t& namespace_name);
        [[nodiscard]] bool namespace_exists(const table_namespace_t& namespace_name) const;

        [[nodiscard]] std::pmr::vector<table_namespace_t> list_root_namespaces() const;
        [[nodiscard]] std::pmr::vector<table_namespace_t> list_child_namespaces(const table_namespace_t& parent) const;
        [[nodiscard]] std::pmr::vector<table_namespace_t> list_all_namespaces() const;

        [[nodiscard]] bool has_child_namespaces(const table_namespace_t& namespace_name) const;
        [[nodiscard]] std::pmr::vector<table_namespace_t>
        get_all_descendants(const table_namespace_t& namespace_name) const;

        [[nodiscard]] namespace_info& get_namespace_info(const table_namespace_t& namespace_name);

        void clear();
        size_t size() const;

    private:
        // TODO: use same approach in our trie for versioning
        struct namespace_info {
            namespace_info(std::pmr::memory_resource* resource)
                : tables(resource) {}

            std::pmr::map<std::pmr::string, table_metadata> tables;
        };

        // TODO: REPLACE with our trie, boost::detail is HORRIBLE
        using trie_type = trie_map<table_namespace_t, namespace_info>;
        table_namespace_t get_parent_namespace(const table_namespace_t& namespace_name) const;

        trie_type namespaces;
        std::pmr::memory_resource* resource;
    };
} // namespace components::catalog

#pragma once

#include "namespace_storage.hpp"
#include "table_metadata.hpp"
#include "transaction/transaction_scope.hpp"

namespace components::catalog {
    class catalog : public std::enable_shared_from_this<catalog> {
    public:
        catalog(std::pmr::memory_resource* resource);

        catalog(const catalog&) = delete;
        catalog(catalog&&) = delete;

        catalog& operator=(const catalog&) = delete;
        catalog& operator=(catalog&&) = delete;

        // namespace operations
        [[nodiscard]] std::pmr::vector<table_namespace_t> list_namespaces() const;
        [[nodiscard]] std::pmr::vector<table_namespace_t> list_namespaces(const table_namespace_t& parent) const;
        [[nodiscard]] bool namespace_exists(const table_namespace_t& namespace_name) const;
        void create_namespace(const table_namespace_t& namespace_name);
        void drop_namespace(const table_namespace_t& namespace_name);

        // table operations
        [[nodiscard]] std::pmr::vector<table_id> list_tables(const table_namespace_t& namespace_name) const;
        [[nodiscard]] const schema& get_table_schema(const table_id& id) const;

        void create_table(const table_id& id, table_metadata meta);
        void drop_table(const table_id& id);
        void rename_table(const table_id& from, std::pmr::string to);
        [[nodiscard]] bool table_exists(const table_id& id) const;

        transaction_scope begin_transaction(const table_id& id);

    private:
        mutable namespace_storage namespaces;
        std::shared_ptr<transaction_list> transactions; // the ONLY strong ref to list
        /*std::pmr::map<table_id, table_metadata> meta;*/
        std::pmr::memory_resource* resource;

        friend class transaction_scope;
    };
} // namespace components::catalog

#include "catalog.hpp"
#include "catalog_exception.hpp"
#include "transaction/transaction_scope.hpp"

namespace components::catalog {
    catalog::catalog(std::pmr::memory_resource* resource)
        : namespaces(resource)
        , transactions(std::make_shared<transaction_list>(resource))
        , resource(resource) {}

    std::pmr::vector<table_namespace_t> catalog::list_namespaces() const { return namespaces.list_root_namespaces(); }

    std::pmr::vector<table_namespace_t> catalog::list_namespaces(const table_namespace_t& parent) const {
        return namespaces.list_child_namespaces(parent);
    }

    bool catalog::namespace_exists(const table_namespace_t& namespace_name) const {
        return namespaces.namespace_exists(namespace_name);
    }

    void catalog::create_namespace(const table_namespace_t& namespace_name) {
        namespaces.create_namespace(namespace_name);
    }

    void catalog::drop_namespace(const table_namespace_t& namespace_name) {
        auto descendants = namespaces.get_all_descendants(namespace_name);

        for (const auto& des : descendants) {
            if (!namespaces.get_namespace_info(des).tables.empty()) {
                throw not_supported_exception("Cannot drop namespace with existing tables: " +
                                              table_id(resource, namespace_name).to_string());
            }
        }

        namespaces.drop_namespace(namespace_name);
    }

    std::pmr::vector<table_id> catalog::list_tables(const table_namespace_t& namespace_name) const {
        if (!namespace_exists(namespace_name)) {
            throw no_such_namespace_exception("Namespace does not exist: " +
                                              table_id(resource, namespace_name).to_string());
        }

        std::pmr::vector<table_id> result(resource);
        const auto& info = namespaces.get_namespace_info(namespace_name).tables;
        result.reserve(info.size());

        for (const auto& meta : info) {
            result.push_back(table_id(resource, namespace_name, meta.first));
        }

        return result;
    }

    const schema& catalog::get_table_schema(const table_id& id) const {
        const auto& info = namespaces.get_namespace_info(id.get_namespace()).tables;
        auto it = info.find(id.table_name());
        assert(it != info.end());
        return it->second.current_schema();
    }

    void catalog::create_table(const table_id& id, table_metadata meta) {
        if (!namespace_exists(id.get_namespace())) {
            throw no_such_namespace_exception("Namespace does not exist for table: " + id.to_string());
        }

        if (table_exists(id)) {
            throw already_exists_exception("Table already exists: " + id.to_string());
        }

        namespaces.get_namespace_info(id.get_namespace()).tables.emplace(id.table_name(), std::move(meta));
    }

    // todo: test drop during transaction
    void catalog::drop_table(const table_id& id) {
        if (!table_exists(id)) {
            throw no_such_table_exception("Table does not exist: " + id.to_string());
        }

        namespaces.get_namespace_info(id.get_namespace()).tables.erase(id.table_name());
    }

    // todo: test rename during transaction
    void catalog::rename_table(const table_id& from, std::pmr::string to) {
        if (!table_exists(from)) {
            throw no_such_table_exception("Source table does not exist: " + from.to_string());
        }

        if (table_exists(table_id(resource, from.get_namespace(), to))) {
            throw already_exists_exception("Target table already exists: " + std::string(to));
        }

        auto& info = namespaces.get_namespace_info(from.get_namespace()).tables;
        auto node = info.extract(from.table_name());
        node.key() = to;
        info.insert(std::move(node));
    }

    bool catalog::table_exists(const table_id& id) const {
        const auto& info = namespaces.get_namespace_info(id.get_namespace()).tables;
        return info.find(id.table_name()) != info.end();
    }

    transaction_scope catalog::begin_transaction(const table_id& id) {
        if (!table_exists(id)) {
            throw no_such_table_exception("Table does not exist: " + std::string(id.table_name()));
        }

        transactions->add_transaction(id);
        return transaction_scope(transactions, id, namespaces, resource);
    }
} // namespace components::catalog

#include "catalog/namespace_storage.hpp"

namespace components::catalog {
    namespace_storage::namespace_storage(std::pmr::memory_resource* resource)
        : namespaces(resource)
        , resource(resource) {}

    void namespace_storage::create_namespace(const table_namespace_t& namespace_name) {
        if (namespace_name.empty()) {
            throw catalog_exception("Namespace name cannot be empty");
        }

        if (namespace_exists(namespace_name)) {
            throw already_exists_exception("Namespace already exists");
        }

        if (namespace_name.size() > 1) {
            auto parent = get_parent_namespace(namespace_name);
            if (!namespace_exists(parent)) {
                throw no_such_namespace_exception("Parent namespace does not exist");
            }
        }

        table_namespace_t path(namespace_name.begin(), namespace_name.end(), resource);
        namespaces.insert({path, namespace_info(resource)});
    }

    void namespace_storage::drop_namespace(const table_namespace_t& namespace_name) {
        if (namespace_name.empty()) {
            throw catalog_exception("Namespace name cannot be empty");
        }

        if (!namespace_exists(namespace_name)) {
            throw no_such_namespace_exception("Namespace does not exist");
        }

        if (has_child_namespaces(namespace_name)) {
            throw not_supported_exception("Cannot drop namespace with child namespaces"); // todo: allow?
        }

        table_namespace_t path(namespace_name.begin(), namespace_name.end(), resource);
        namespaces.erase(path);
    }

    bool namespace_storage::namespace_exists(const table_namespace_t& namespace_name) const {
        if (namespace_name.empty()) {
            return false;
        }

        table_namespace_t path(namespace_name.begin(), namespace_name.end(), resource);
        auto it = namespaces.find(path);
        return it != namespaces.end();
    }

    // todo: reuse list_child
    std::pmr::vector<table_namespace_t> namespace_storage::list_root_namespaces() const {
        std::pmr::vector<table_namespace_t> result(resource);

        for (auto it = namespaces.begin(); it != namespaces.end(); ++it) {
            if (it->key.size() == 1) {
                auto a = it->key;
                table_namespace_t ns(a.begin(), a.end(), resource);
                result.push_back(std::move(ns));
            }
        }

        return result;
    }

    std::pmr::vector<table_namespace_t>
    namespace_storage::list_child_namespaces(const table_namespace_t& parent) const {
        if (!namespace_exists(parent)) {
            throw no_such_namespace_exception("Parent namespace does not exist");
        }

        std::pmr::vector<table_namespace_t> result(resource);
        table_namespace_t next(resource);

        if (auto res = namespaces.longest_match(parent); res.match && !res.leaf) {
            namespaces.copy_next_key_elements(res, std::back_inserter(next));
            result.reserve(next.size());

            for (auto&& ext : next) {
                table_namespace_t extended = parent;
                extended.emplace_back(std::move(ext));
                result.emplace_back(std::move(extended));
            }
        }

        return result;
    }

    std::pmr::vector<table_namespace_t> namespace_storage::list_all_namespaces() const {
        std::pmr::vector<table_namespace_t> result(resource);

        for (const auto& [path, info] : namespaces) {
            table_namespace_t ns(path.begin(), path.end(), resource);
            result.push_back(std::move(ns));
        }

        return result;
    }

    bool namespace_storage::has_child_namespaces(const table_namespace_t& namespace_name) const {
        auto match_res = namespaces.longest_match(namespace_name);
        return !match_res.leaf;
    }

    std::pmr::vector<table_namespace_t>
    namespace_storage::get_all_descendants(const table_namespace_t& namespace_name) const {
        if (!namespace_exists(namespace_name)) {
            throw no_such_namespace_exception("Namespace does not exist");
        }

        std::pmr::vector<table_namespace_t> result(resource);
        std::pmr::vector<table_namespace_t> stack(resource); // recursion may overflow?

        stack.push_back(namespace_name);
        while (!stack.empty()) {
            auto current = std::move(stack.back());
            stack.pop_back();

            auto children = list_child_namespaces(current);
            for (auto&& child : children) {
                stack.push_back(child);
                result.emplace_back(std::move(child));
            }
        }

        return result;
    }

    namespace_storage::namespace_info&
    namespace_storage::get_namespace_info(const components::catalog::table_namespace_t& namespace_name) {
        if (!namespace_exists(namespace_name)) {
            throw no_such_namespace_exception("Namespace does not exist");
        }

        return namespaces.find(namespace_name)->value;
    }

    void namespace_storage::clear() { namespaces.clear(); }

    size_t namespace_storage::size() const { return static_cast<size_t>(namespaces.size()); }

    table_namespace_t namespace_storage::get_parent_namespace(const table_namespace_t& namespace_name) const {
        if (namespace_name.empty()) {
            return table_namespace_t(resource);
        }

        table_namespace_t parent(namespace_name.begin(), namespace_name.end() - 1, resource);
        return parent;
    }
} // namespace components::catalog

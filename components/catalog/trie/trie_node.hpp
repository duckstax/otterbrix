#pragma once

#include <algorithm>
#include <cassert>
#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

// Enum для типов колонок
enum class column_type_t
{
    int_type,
    string_type,
    object_type,
    array_type
};

struct version_entry {
    uint64_t version;
    std::set<column_type_t> types; // todo: polymorph
    mutable uint32_t ref_count = 0;

    version_entry(uint64_t v, const std::set<column_type_t>& t)
        : version(v)
        , types(t) {}

    void add_ref() const { ++ref_count; }

    void release_ref() const {
        if (ref_count > 0) {
            --ref_count;
        }
    }

    bool is_alive() const { return ref_count > 0; }
};

// Узел префиксного дерева
struct trie_node {
    std::string prefix;
    std::vector<version_entry> versions;
    std::vector<std::unique_ptr<trie_node>> children;
    trie_node* parent = nullptr;

    trie_node(const std::string& p = "", trie_node* par = nullptr)
        : prefix(p)
        , parent(par) {}

    // Найти дочерний узел по первому символу
    trie_node* find_child(char c) {
        auto it = std::find_if(children.begin(), children.end(), [c](const std::unique_ptr<trie_node>& child) {
            return !child->prefix.empty() && child->prefix[0] == c;
        });
        return it != children.end() ? it->get() : nullptr;
    }

    // Получить последнюю версию
    version_entry* get_latest_version() {
        if (versions.empty())
            return nullptr;
        return &versions.back();
    }

    // Добавить новую версию
    void add_version(uint64_t version, const std::set<column_type_t>& types) { versions.emplace_back(version, types); }

    // Очистить мертвые версии
    void cleanup_dead_versions() {
        versions.erase(
            std::remove_if(versions.begin(), versions.end(), [](const version_entry& v) { return !v.is_alive(); }),
            versions.end());
    }

    bool has_live_versions() const {
        return std::any_of(versions.begin(), versions.end(), [](const version_entry& v) { return v.is_alive(); });
    }
};

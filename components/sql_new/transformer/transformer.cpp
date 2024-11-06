#include "transformer.hpp"

namespace components::sql_new::transform {
    transformer& transformer::get_root() {
        std::reference_wrapper<transformer> node = *this;
        while (node.get().parent) {
            node = *node.get().parent;
        }
        return node.get();
    }

    const transformer& transformer::get_root() const {
        std::reference_wrapper<const transformer> node = *this;
        while (node.get().parent) {
            node = *node.get().parent;
        }
        return node.get();
    }

    void transformer::set_param(size_t key, size_t value) {
        auto& root = get_root();
        assert(!root.named_param_map.count(key));
        root.named_param_map[key] = value;
    }

    bool transformer::get_param(size_t key, size_t& value) {
        auto& root = get_root();
        auto entry = root.named_param_map.find(key);
        if (entry == root.named_param_map.end()) {
            return false;
        }
        value = entry->second;
        return true;
    }

    size_t transformer::get_param_count() const {
        auto& root = get_root();
        return root.prepared_statement_parameter_index;
    }

    void transformer::set_param_count(size_t new_count) {
        auto& root = get_root();
        root.prepared_statement_parameter_index = new_count;
    }
} // namespace components::sql_new::transform

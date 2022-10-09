#pragma once

#include "storage.hpp"

namespace components::ql::aggregate {

    struct operators_t final {
        operators_t() = default;
        explicit operators_t(std::size_t size);
        void append(const std::string& name, aggregate_types step, operator_storage_t);
        struct operator_t final {
            operator_t(const std::string& name, const aggregate_types step, operator_storage_t storage);
            std::string name_;
            aggregate_types type_;
            operator_storage_t operator_;
        };

        std::vector<operator_t> operators;
    };

    using operators_ptr = std::unique_ptr<operators_t>;

    template<class... Args>
    void group(const operators_ptr& ptr, Args&&... args) {
        ptr->append("group", aggregate_types::group, group_t(std::forward<Args>(args)...));
    }

} // namespace components::ql::aggregate
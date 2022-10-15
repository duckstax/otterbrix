#pragma once

#include "storage.hpp"

namespace components::ql::aggregate {

    class operators_t final {
    public:
        operators_t() = default;
        explicit operators_t(std::size_t size);
        void append(operator_type step, operator_storage_t);
        void reserve(std::size_t size);

    private:
        struct operator_t final {
            operator_t(const operator_type step, operator_storage_t storage);
            operator_type type_;
            operator_storage_t operator_;
        };

        std::vector<operator_t> operators;
    };

    using operators_ptr = std::unique_ptr<operators_t>;

    template<class... Args>
    operators_ptr make_operators(Args&&... args) {
        return std::make_unique<operators_t>(std::forward<Args>(args)...);
    }

    template<class... Args>
    void group(const operators_ptr& ptr, Args&&... args) {
        ptr->append(operator_type::group, group_t(std::forward<Args>(args)...));
    }

} // namespace components::ql::aggregate
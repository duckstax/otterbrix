#pragma once

#include "storage.hpp"

namespace components::ql::aggregate {

    class operators_t final {
    public:
        operators_t() = default;
        explicit operators_t(std::size_t size);
        void append(operator_type type, operator_storage_t storage);
        void reserve(std::size_t size);

    private:
        struct operator_t final {
            operator_t(operator_type type, operator_storage_t storage);
            const operator_type type_;
            operator_storage_t operator_;
        };

        std::vector<operator_t> operators;
    };

} // namespace components::ql::aggregate
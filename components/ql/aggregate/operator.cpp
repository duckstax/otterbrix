#include "operator.hpp"

namespace components::ql::aggregate {

    operators_t::operators_t(std::size_t size) {
        operators.reserve(size);
    }
    void operators_t::append(operator_type type, operator_storage_t storage) {
        operators.emplace_back(operator_t{type, std::move(storage)});
    }

    void operators_t::reserve(std::size_t size) {
        operators.reserve(size);
    }

    operators_t::operator_t::operator_t(operator_type type, operator_storage_t storage)
        : type_(type)
        , operator_(std::move(storage)) {
    }
} // namespace components::ql::aggregate
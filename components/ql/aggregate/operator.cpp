#include "operator.hpp"

namespace components::ql::aggregate {

    operators_t::operators_t(std::size_t size) {
        operators.reserve(size);
    }
    void operators_t::append(operator_type step, operator_storage_t expr) {
        operators.emplace_back(operator_t{step, std::move(expr)});
    }

    void operators_t::reserve(std::size_t size) {
        operators.reserve(size);
    }

    operators_t::operator_t::operator_t(const operator_type step, operator_storage_t storage)
        : type_(step)
        , operator_(std::move(storage)) {
    }
} // namespace components::ql::aggregate
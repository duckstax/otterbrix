#include "operator.hpp"

namespace components::ql::aggregate {

    operators_t::operators_t(std::size_t size) {
        operators.reserve(size);
    }
    void operators_t::append(const std::string& name, aggregate_types step, operator_storage_t expr) {
        operators.emplace_back(operator_t{name, step, std::move(expr)});
    }

    operators_t::operator_t::operator_t(const std::string& name, const aggregate_types step, operator_storage_t storage)
        : name_(name)
        , type_(step)
        , operator_(std::move(storage)) {
    }
} // namespace components::ql::aggregate
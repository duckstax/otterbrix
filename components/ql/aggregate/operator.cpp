#include "operator.hpp"

namespace components::ql::aggregate {

    operators_t::operators_t(std::size_t size) { operators_.reserve(size); }
    void operators_t::append(operator_type type, operator_storage_t storage) {
        operators_.emplace_back(operator_t{type, std::move(storage)});
    }

    void operators_t::reserve(std::size_t size) { operators_.reserve(size); }

    std::size_t operators_t::size() const { return operators_.size(); }

    operator_type operators_t::type(std::size_t index) const { return operators_.at(index).type_; }

    operators_t::operator_t::operator_t(operator_type type, operator_storage_t storage)
        : type_(type)
        , operator_(std::move(storage)) {}
} // namespace components::ql::aggregate
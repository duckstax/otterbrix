#include "param_storage.hpp"

#include <core/pmr.hpp>

namespace components::logical_plan {

    const expr_value_t& get_parameter(const storage_parameters* storage, core::parameter_id_t id) {
        auto it = storage->parameters.find(id);
        if (it != storage->parameters.end()) {
            return it->second;
        }
        // TODO compile error
        return expr_value_t{};
    }

    auto ql_param_statement_t::parameters() const -> const storage_parameters& { return values_; }

    storage_parameters ql_param_statement_t::take_parameters() { return std::move(values_); }

    auto ql_param_statement_t::set_parameters(const storage_parameters& parameters) -> void { values_ = parameters; }

    auto ql_param_statement_t::next_id() -> core::parameter_id_t {
        auto tmp = counter_;
        counter_ += 1;
        return core::parameter_id_t(tmp);
    }

    auto ql_param_statement_t::parameter(core::parameter_id_t id) const -> const expr_value_t& {
        return get_parameter(&values_, id);
    }

    ql_param_statement_ptr make_ql_param_statement(std::pmr::memory_resource* resource) {
        return {new ql_param_statement_t(resource)};
    }
} // namespace components::logical_plan

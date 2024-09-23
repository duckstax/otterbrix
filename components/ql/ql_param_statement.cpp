#include "ql_param_statement.hpp"

#include <core/pmr.hpp>

namespace components::ql {

    const expr_value_t& get_parameter(const storage_parameters* storage, core::parameter_id_t id) {
        auto it = storage->parameters.find(id);
        if (it != storage->parameters.end()) {
            return it->second;
        }
        // TODO compile error
        return expr_value_t{};
    }

    ql_param_statement_t::ql_param_statement_t(statement_type type,
                                               database_name_t database,
                                               collection_name_t collection,
                                               std::pmr::memory_resource* resource)
        : ql_statement_t(type, std::move(database), std::move(collection))
        , values_(resource) {}

    bool ql_param_statement_t::is_parameters() const { return true; }

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
} // namespace components::ql

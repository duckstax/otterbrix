#include "ql_param_statement.hpp"

namespace components::ql {

    ql_param_statement_t::ql_param_statement_t(statement_type type, database_name_t database, collection_name_t collection)
        : ql_statement_t(type, std::move(database), std::move(collection)) {}

    auto ql_param_statement_t::parameters() -> const storage_parameters& {
        return values_;
    }

    auto ql_param_statement_t::next_id() -> core::parameter_id_t {
        auto tmp = counter_;
        counter_ += 1;
        return core::parameter_id_t(tmp);
    }

    auto ql_param_statement_t::parameter(core::parameter_id_t id) const -> const expr_value_t& {
        auto it = values_.find(id);
        if (it != values_.end()) {
            return it->second;
        }
        static expr_value_t null_(nullptr);
        return null_;
    }

    template<>
    void ql_param_statement_t::add_parameter(core::parameter_id_t id, expr_value_t value) {
        values_.emplace(id, value);
    }

    template<>
    void ql_param_statement_t::add_parameter(core::parameter_id_t id, const ::document::impl::value_t* value) {
        values_.emplace(id, expr_value_t(value));
    }

    template<>
    void ql_param_statement_t::add_parameter(core::parameter_id_t id, const std::string& value) {
        values_.emplace(id, expr_value_t(::document::impl::new_value(value).detach()));
    }

} // namespace components::ql

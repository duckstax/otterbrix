#include "ql_param_statement.hpp"

namespace components::ql {

    template<>
    void add_parameter(storage_parameters &storage, core::parameter_id_t id, expr_value_t value) {
        storage.emplace(id, value);
    }

    template<>
    void add_parameter(storage_parameters &storage, core::parameter_id_t id, const ::document::impl::value_t* value) {
        storage.emplace(id, expr_value_t(value));
    }

    const expr_value_t& get_parameter(const storage_parameters *storage, core::parameter_id_t id) {
        auto it = storage->find(id);
        if (it != storage->end()) {
            return it->second;
        }
        static const expr_value_t null_value = expr_value_t{nullptr};
        return null_value;
    }


    ql_param_statement_t::ql_param_statement_t(statement_type type, database_name_t database, collection_name_t collection)
        : ql_statement_t(type, std::move(database), std::move(collection)) {}

    bool ql_param_statement_t::is_parameters() const {
        return true;
    }

    auto ql_param_statement_t::parameters() const -> const storage_parameters& {
        return values_;
    }

    storage_parameters ql_param_statement_t::take_parameters() {
        return std::move(values_);
    }

    auto ql_param_statement_t::set_parameters(const storage_parameters& parameters) -> void {
        values_ = parameters;
    }

    auto ql_param_statement_t::next_id() -> core::parameter_id_t {
        auto tmp = counter_;
        counter_ += 1;
        return core::parameter_id_t(tmp);
    }

    auto ql_param_statement_t::parameter(core::parameter_id_t id) const -> const expr_value_t& {
        return get_parameter(&values_, id);
    }

} // namespace components::ql

#pragma once

#include <components/document/mutable/mutable_value.hpp>
#include <components/expressions/forward.hpp>
#include "ql_statement.hpp"

namespace components::ql {

    using expr_value_t = ::document::wrapper_value_t;
    using storage_parameters = std::unordered_map<core::parameter_id_t, expr_value_t>;

    template<class Value>
    void add_parameter(storage_parameters &storage, core::parameter_id_t id, Value value) {
        storage.emplace(id, expr_value_t(::document::impl::new_value(value).detach()));
    }


    class ql_param_statement_t : public ql_statement_t {
    public:
        ql_param_statement_t(statement_type type, database_name_t database, collection_name_t collection);

        auto parameters() -> const storage_parameters&;

        auto next_id() -> core::parameter_id_t;

        template<class Value>
        void add_parameter(core::parameter_id_t id, Value value) {
            add_parameter(values_, id, value);
        }

        template<class Value>
        core::parameter_id_t add_parameter(Value value) {
            auto id = next_id();
            add_parameter(id, value);
            return id;
        }

        auto parameter(core::parameter_id_t id) const -> const expr_value_t&;

    private:
        uint16_t counter_{0};
        storage_parameters values_;
    };

} // namespace components::ql

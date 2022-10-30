#pragma once

#include <cstdint>

#include <map>
#include <variant>
#include <vector>

#include "aggregate/operator.hpp"
#include "ql/expr2.hpp"
#include "ql/ql_statement.hpp"

namespace components::ql {

    aggregate::operator_type get_aggregate_type(const std::string&);

    class aggregate_statement final : public ql_statement_t {
    public:
        aggregate_statement(database_name_t database, collection_name_t collection);

        void append(aggregate::operator_type type, aggregate::operator_storage_t storage);

//        template<class... Args>
//        void append(Args&&... args) {
//            aggregate_operator_.append(std::forward<Args>(args)...);
//        }

        void reserve(std::size_t size) {
            aggregate_operator_.reserve(size);
        }

        auto next_id() -> core::parameter_id_t { ///change return core::parameter_id_t(counter_ ++);
            auto tmp = counter_;
            counter_ += 1;
            return core::parameter_id_t(tmp);
        }

        template<class Value>
        void add_parameter(core::parameter_id_t id, Value value) {
            values_.emplace(id, expr_value_t(::document::impl::new_value(value).detach()));
        }

    private:
        aggregate::operators_t aggregate_operator_;
        uint16_t counter_{0};
        std::unordered_map<core::parameter_id_t, expr_value_t> values_;
    };

} // namespace components::ql
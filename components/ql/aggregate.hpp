#pragma once

#include <cstdint>

#include <map>
#include <variant>
#include <vector>

#include "aggregate/operator.hpp"
#include "ql/experimental/expr.hpp"
#include "ql/ql_statement.hpp"

namespace components::ql {

    aggregate::operator_type get_aggregate_type(const std::string&);

    class aggregate_statement final : public ql_statement_t {
        using storage_parameters = std::unordered_map<core::parameter_id_t, expr_value_t>;

    public:
        aggregate_statement(database_name_t database, collection_name_t collection);

        void append(aggregate::operator_type type, aggregate::operator_storage_t storage);

//        template<class... Args>
//        void append(Args&&... args) {
//            aggregate_operator_.append(std::forward<Args>(args)...);
//        }

        void reserve(std::size_t size);

        auto next_id() -> core::parameter_id_t; ///change return core::parameter_id_t(counter_ ++);

        template<class Value>
        void add_parameter(core::parameter_id_t id, Value value) {
            values_.emplace(id, expr_value_t(::document::impl::new_value(value).detach()));
        }

        template<class Value>
        core::parameter_id_t add_parameter(Value value) {
            auto id = next_id();
            add_parameter(id, value);
            return id;
        }

        auto parameter(core::parameter_id_t id) const -> const expr_value_t&;

        auto count_operators() const -> std::size_t;
        auto type_operator(std::size_t index) const -> aggregate::operator_type;

        template <class T>
        const T &get_operator(std::size_t index) const {
            return aggregate_operator_.get<T>(index);
        }

    private:
        aggregate::operators_t aggregate_operator_;
        uint16_t counter_{0};
        storage_parameters values_;
    };


    template <class OStream>
    OStream &operator<<(OStream &stream, const aggregate_statement &aggregate) {
        stream << "$aggregate: {";
        bool is_first = true;
        for (std::size_t i = 0; i < aggregate.count_operators(); ++i) {
            if (is_first) {
                is_first = false;
            } else {
                stream << ", ";
            }
            switch (aggregate.type_operator(i)) {
                case aggregate::operator_type::match:
                    stream << aggregate.get_operator<aggregate::match_t>(i);
                    break;
                case aggregate::operator_type::group:
                    stream << aggregate.get_operator<aggregate::group_t>(i);
                    break;
                case aggregate::operator_type::sort:
                    stream << aggregate.get_operator<aggregate::sort_t>(i);
                    break;
                default:
                    break;
            }
        }
        stream << "}";
        return stream;
    }

} // namespace components::ql
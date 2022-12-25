#pragma once

#include <cstdint>

#include <map>
#include <variant>
#include <vector>

#include <components/expressions/expression.hpp>
#include <components/ql/ql_param_statement.hpp>
#include "aggregate/operator.hpp"

namespace components::ql {

    aggregate::operator_type get_aggregate_type(const std::string&);

    class aggregate_statement final : public ql_param_statement_t {
    public:
        aggregate_statement(database_name_t database, collection_name_t collection);

        void append(aggregate::operator_type type, aggregate::operator_storage_t storage);

        void reserve(std::size_t size);

        auto count_operators() const -> std::size_t;
        auto type_operator(std::size_t index) const -> aggregate::operator_type;

        template <class T>
        const T &get_operator(std::size_t index) const {
            return aggregate_operator_.get<T>(index);
        }

    private:
        aggregate::operators_t aggregate_operator_;
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


    using aggregate_statement_ptr = std::unique_ptr<aggregate_statement>;

    aggregate_statement_ptr make_aggregate_statement(const database_name_t &database, const collection_name_t &collection);

} // namespace components::ql
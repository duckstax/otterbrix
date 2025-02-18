#pragma once

#include <cstdint>

#include <map>
#include <variant>
#include <vector>

#include "aggregate/operator.hpp"
#include <components/expressions/expression.hpp>
#include <components/ql/ql_param_statement.hpp>

namespace components::ql {

    aggregate::operator_type get_aggregate_type(const std::string&);

    class aggregate_statement final : public ql_param_statement_t {
    public:
        ql_statement_ptr data{nullptr};

        aggregate_statement(database_name_t database,
                            collection_name_t collection,
                            std::pmr::memory_resource* resource);

        explicit aggregate_statement(std::pmr::memory_resource* resource);

        void append(aggregate::operator_type type, aggregate::operator_storage_t storage);

        void reserve(std::size_t size);

        auto count_operators() const -> std::size_t;
        auto type_operator(std::size_t index) const -> aggregate::operator_type;

        template<class T>
        const T& get_operator(std::size_t index) const {
            return aggregate_operator_.get<T>(index);
        }

        std::string to_string() const final;

    private:
        aggregate::operators_t aggregate_operator_;
    };

    template<class OStream>
    OStream& operator<<(OStream& stream, const aggregate_statement& aggregate) {
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

    using aggregate_ptr = boost::intrusive_ptr<aggregate_statement>;
    using aggregate_statement_raw_ptr = aggregate_statement*;

    aggregate_ptr make_aggregate(const database_name_t& database,
                                 const collection_name_t& collection,
                                 std::pmr::memory_resource* resource);

    const components::ql::aggregate::match_t& get_match(const aggregate_statement& aggregate);

} // namespace components::ql

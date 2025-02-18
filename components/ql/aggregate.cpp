#include "aggregate.hpp"
#include <magic_enum.hpp>

namespace components::ql {

    aggregate_statement::aggregate_statement(database_name_t database,
                                             collection_name_t collection,
                                             std::pmr::memory_resource* resource)
        : ql_param_statement_t(statement_type::aggregate, std::move(database), std::move(collection), resource) {}

    aggregate_statement::aggregate_statement(std::pmr::memory_resource* resource)
        : ql_param_statement_t(statement_type::aggregate, "", "", resource) {}

    void aggregate_statement::append(aggregate::operator_type type, aggregate::operator_storage_t storage) {
        aggregate_operator_.append(type, std::move(storage));
    }

    void aggregate_statement::reserve(std::size_t size) { aggregate_operator_.reserve(size); }

    aggregate::operator_type get_aggregate_type(const std::string& key) {
        auto type = magic_enum::enum_cast<aggregate::operator_type>(key);

        if (type.has_value()) {
            return type.value();
        }

        return aggregate::operator_type::invalid;
    }

    auto aggregate_statement::count_operators() const -> std::size_t { return aggregate_operator_.size(); }

    auto aggregate_statement::type_operator(std::size_t index) const -> aggregate::operator_type {
        return aggregate_operator_.type(index);
    }

    std::string aggregate_statement::to_string() const {
        std::stringstream s;
        s << "aggregate: " << database_ << "." << collection_;
        return s.str();
    }

    aggregate_ptr make_aggregate(const database_name_t& database,
                                 const collection_name_t& collection,
                                 std::pmr::memory_resource* resource) {
        return new aggregate_statement(database, collection, resource);
    }

    const aggregate::match_t& get_match(const aggregate_statement& aggregate) {
        for (std::size_t i = 0; i < aggregate.count_operators(); ++i) {
            if (aggregate.type_operator(i) == aggregate::operator_type::match) {
                return aggregate.get_operator<aggregate::match_t>(i);
            }
        }

        static aggregate::match_t null_match;
        return null_match;
    }

} // namespace components::ql

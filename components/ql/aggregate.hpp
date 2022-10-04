#pragma once

#include <variant>
#include <vector>

#include <magic_enum.hpp>

#include "expr.hpp"
#include "ql_statement.hpp"

namespace components::ql {

    enum class aggregate_op_steps : uint16_t {
        novalid = 1,
        count, ///group + project
        densify,
        facet,
        fill,
        group,
        limit,
        lookup,
        match,
        merge,
        out,
        project,
        redact,
        sample,
        search,
        set,
        skip,
        sort,
        sortByCount,
        unionWith,
        unset,
        unwind,
        finish
    };

    std::string to_string(aggregate_op_steps statement);
    aggregate_op_steps from_string(std::string statement);

    constexpr static auto count_steps = static_cast<uint16_t>(aggregate_op_steps::finish) - static_cast<uint16_t>(aggregate_op_steps::novalid);
/*
    struct group_t final {
        aggregate_op_steps statement = aggregate_op_steps::group;
    };

    struct match_t final {
    };

    struct merge_t final {
    };

    class aggregate_steps_t final {
    public:

        aggregate_steps_t(){}

        template<class Target>
        const Target& get() const {
            auto id = magic_enum::enum_integer(Target::statement);
            return std::get<id>(storage_);
        }

    private:
        using storage_t = std::variant<
            group_t,
            match_t,
            merge_t>;
        storage_t storage_;
        aggregate_steps_statement aggregate_steps_statement_;
    };

    using aggregate_step_storage_t = std::vector<aggregate_steps_t>;

*/

    struct aggregate_operator_t {
        aggregate_operator_t() =default;
        std::string name;
        aggregate_op_steps op;
        expr_ptr expr;
    };

    using aggregate_operator_ptr = std::unique_ptr<aggregate_operator_t>;

    aggregate_operator_ptr make_aggregate_operator(std::string name, aggregate_op_steps op_type, expr_ptr ptr) {
        auto op = std::make_unique<aggregate_operator_t>();

        op->name = name;
        op->op = op_type;
        op->expr = std::move(ptr);

        return op;
    }

    class aggregate_statement final : public ql_statement_t {
    public:
        aggregate_statement(database_name_t database, collection_name_t collection)
            : ql_statement_t(statement_type::aggregate, std::move(database), std::move(collection)) {}


        void append(aggregate_operator_ptr ptr){
            aggregate_operator_.push_back(std::move(ptr));
        }

    private:
        std::vector<aggregate_operator_ptr> aggregate_operator_;

    };

} // namespace components::ql
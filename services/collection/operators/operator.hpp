#pragma once

#include <collection/operators/predicates/predicate.hpp>

#include <collection/operators/predicates/limit.hpp>
#include <components/ql/ql_statement.hpp>
#include <services/collection/collection.hpp>

namespace services::collection::operators {

    using operator_type = components::ql::ql_statement_t;

    class operator_t {
    public:
        operator_t() = delete;
        operator_t(const operator_t&) = delete;
        operator_t& operator=(const operator_t&) = delete;
        operator_t(context_collection_t* collection);
        virtual ~operator_t() = default;

        void on_execute(const predicate_ptr&, predicates::limit_t, components::cursor::sub_cursor_t*);

    protected:
        context_collection_t* context_;

    private:
        virtual void on_execute_impl(const predicate_ptr&, predicates::limit_t, components::cursor::sub_cursor_t*) = 0;

        const operator_type operator_type_;
        operator_t* left_input_;
        operator_t* right_input_;
    };

    class read_operator_t : public operator_t {
    };

    enum class read_write_operator_state {
        pending,
        executed,
        conflicted,
        rolledBack,
        committed
    };

    class read_write_operator_t : public operator_t {

        read_write_operator_state state_;
    };

    using operator_ptr = std::unique_ptr<operator_t>;

} // namespace services::collection::operators

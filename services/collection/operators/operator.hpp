#pragma once

#include <services/collection/collection.hpp>
#include <services/collection/operators/operator_data.hpp>
#include <services/collection/planner/transaction_context.hpp>

namespace services::collection::operators {

    enum class operator_type {
        unused = 0x0,
        empty,
        match,
        insert,
        remove,
        update,
        aggregate
    };

    enum class operator_state {
        created,
        running,
        executed,
        cleared
    };

    class operator_t {
    public:
        using ptr = std::unique_ptr<operator_t>;

        operator_t() = delete;
        operator_t(const operator_t&) = delete;
        operator_t& operator=(const operator_t&) = delete;
        operator_t(context_collection_t* collection, operator_type type);
        virtual ~operator_t() = default;

        void on_execute(planner::transaction_context_t* transaction_context);

        operator_state state() const;
        const operator_data_ptr& output() const;
        void set_children(ptr left, ptr right = nullptr);

    protected:
        context_collection_t* context_;
        ptr left_  {nullptr};
        ptr right_ {nullptr};
        operator_data_ptr output_ {nullptr};

    private:
        virtual void on_execute_impl(planner::transaction_context_t* transaction_context) = 0;

        const operator_type type_;
        operator_state state_ {operator_state::created};
    };

    class read_only_operator_t : public operator_t {
    public:
        read_only_operator_t(context_collection_t* collection, operator_type type);
    };

    enum class read_write_operator_state {
        pending,
        executed,
        conflicted,
        rolledBack,
        committed
    };

    class read_write_operator_t : public operator_t {
    public:
        read_write_operator_t(context_collection_t* collection, operator_type type);
        //todo:
        //void commit();
        //void rollback();

    private:
        read_write_operator_state state_;
    };

    using operator_ptr = operator_t::ptr;

} // namespace services::collection::operators

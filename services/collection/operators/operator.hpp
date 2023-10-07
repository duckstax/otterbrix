#pragma once

#include <components/pipeline/context.hpp>
#include <services/collection/operators/operator_data.hpp>
#include <services/collection/operators/operator_write_data.hpp>

namespace services::collection {

    class context_collection_t;

} // namespace services::collection

namespace services::collection::operators {

    enum class operator_type {
        unused = 0x0,
        empty,
        match,
        insert,
        remove,
        update,
        sort,
        aggregate
    };

    enum class operator_state {
        created,
        running,
        waiting,
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

        void on_execute(components::pipeline::context_t* pipeline_context);
        void on_resume(components::pipeline::context_t* pipeline_context);
        void async_wait();

        bool is_executed() const;
        bool is_wait_sync_disk() const;

        [[nodiscard]] operator_state state() const noexcept;
        [[nodiscard]] operator_type type() const noexcept;
        const operator_data_ptr& output() const;
        const operator_write_data_ptr& modified() const;
        const operator_write_data_ptr& no_modified() const;
        void set_children(ptr left, ptr right = nullptr);
        void take_output(ptr &src);
        void clear(); //todo: replace by copy

    protected:
        context_collection_t* context_;
        ptr left_  {nullptr};
        ptr right_ {nullptr};
        operator_data_ptr output_ {nullptr};
        operator_write_data_ptr modified_ {nullptr};
        operator_write_data_ptr no_modified_ {nullptr};

    private:
        virtual void on_execute_impl(components::pipeline::context_t* pipeline_context) = 0;
        virtual void on_resume_impl(components::pipeline::context_t* pipeline_context);
        virtual void on_prepare_impl();

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

    ::document::wrapper_value_t get_value_from_document(const components::document::document_ptr &doc, const components::expressions::key_t &key);

} // namespace services::collection::operators

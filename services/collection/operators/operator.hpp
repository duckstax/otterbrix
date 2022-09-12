#pragma once

#include <collection/operators/predicates/predicate.hpp>

#include <collection/operators/predicates/limit.hpp>
#include <components/ql/ql_statement.hpp>
#include <services/collection/collection.hpp>

namespace services::collection::operators {

    enum class operator_type {
        unused = 0x0,
        full_scan,
        search_by_index,
        insert,
        remove,
        update
    };

    class operator_data_t : public boost::intrusive::list_base_hook<> {
        using document_ptr = components::document::document_ptr;

    public:
        explicit operator_data_t(std::pmr::memory_resource* resource);

        std::size_t size() const;
        std::pmr::vector<document_ptr>& documents();
        void append(document_ptr document);

    private:
        std::pmr::vector<document_ptr> documents_;
    };

    class operator_t {
    public:
        operator_t() = delete;
        operator_t(const operator_t&) = delete;
        operator_t& operator=(const operator_t&) = delete;
        operator_t(context_collection_t* collection, operator_type type);
        virtual ~operator_t() = default;

        void on_execute(operator_data_t* data);

    protected:
        context_collection_t* context_;

    private:
        virtual void on_execute_impl(operator_data_t*) = 0;

        const operator_type operator_type_;
        operator_t* left_input_  {nullptr};
        operator_t* right_input_ {nullptr};
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

    using operator_ptr = std::unique_ptr<operator_t>;

} // namespace services::collection::operators
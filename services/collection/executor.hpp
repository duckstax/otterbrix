#pragma once

#include <services/collection/collection.hpp>
#include <services/memory_storage/context_storage.hpp>

namespace services::collection::executor {

    struct plan_t {
        std::stack<collection::operators::operator_ptr> sub_plans;
        components::ql::storage_parameters parameters;
        services::context_storage_t context_storage_;

        explicit plan_t(std::stack<collection::operators::operator_ptr>&& sub_plans,
                        components::ql::storage_parameters parameters,
                        services::context_storage_t&& context_storage);
    };
    using plan_storage_t = core::pmr::btree::btree_t<components::session::session_id_t, plan_t>;

    class executor_t final : public actor_zeta::basic_async_actor {
    public:
        executor_t(services::memory_storage_t* memory_storage, std::pmr::memory_resource* resource, log_t&& log);
        ~executor_t() = default;

        void execute_plan(const components::session::session_id_t& session,
                          components::logical_plan::node_ptr logical_plan,
                          components::ql::storage_parameters parameters,
                          services::context_storage_t&& context_storage);

        void create_documents(session_id_t& session,
                              context_collection_t* collection,
                              std::pmr::vector<document_ptr>& documents);

        void create_index_finish(const session_id_t& session,
                                 const std::string& name,
                                 const actor_zeta::address_t& index_address,
                                 context_collection_t* collection);
        void create_index_finish_index_exist(const session_id_t& session,
                                             const std::string& name,
                                             context_collection_t* collection);
        void index_modify_finish(const session_id_t& session, context_collection_t* collection);
        void index_find_finish(const session_id_t& session,
                               const std::pmr::vector<document_id_t>& result,
                               context_collection_t* collection);

    private:
        void traverse_plan_(const components::session::session_id_t& session,
                            collection::operators::operator_ptr&& plan,
                            components::ql::storage_parameters&& parameters,
                            services::context_storage_t&& context_storage);

        void execute_sub_plan_(const components::session::session_id_t& session,
                               collection::operators::operator_ptr plan,
                               components::ql::storage_parameters parameters);

        void execute_sub_plan_finish_(const components::session::session_id_t& session,
                                      components::cursor::cursor_t_ptr cursor);

        void execute_plan_finish_(const components::session::session_id_t& session,
                                  components::cursor::cursor_t_ptr&& cursor);

        void aggregate_document_impl(const components::session::session_id_t& session,
                                     context_collection_t* context_,
                                     operators::operator_ptr plan);
        void update_document_impl(const components::session::session_id_t& session,
                                  context_collection_t* context_,
                                  operators::operator_ptr plan);
        void insert_document_impl(const components::session::session_id_t& session,
                                  context_collection_t* context_,
                                  operators::operator_ptr plan);
        void delete_document_impl(const components::session::session_id_t& session,
                                  context_collection_t* context_,
                                  operators::operator_ptr plan);

        context_collection_t* find_context_(const components::session::session_id_t& session);

        services::memory_storage_t* memory_storage_{nullptr};
        std::pmr::memory_resource* resource_;
        plan_storage_t plans_;
        log_t log_;
    };

} // namespace services::collection::executor
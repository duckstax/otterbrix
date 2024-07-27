#pragma once
#include <components/document/document.hpp>
#include <components/ql/index.hpp>
#include <components/session/session.hpp>
#include <services/collection/collection.hpp>

namespace services::collection {

    bool try_update_index_compare(const components::document::document_ptr& doc,
                                  components::ql::create_index_ptr& index_ql);

    void create_index_impl(context_collection_t* context,
                           components::pipeline::context_t* pipeline_context,
                           components::ql::create_index_ptr index_ql,
                           bool is_pending = false);

    void create_pending_index_impl(context_collection_t* context,
                                   components::pipeline::context_t* pipeline_context,
                                   components::ql::create_index_ptr index_ql);

    void process_pending_indexes(context_collection_t* context);

} // namespace services::collection
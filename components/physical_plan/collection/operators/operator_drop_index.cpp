#include "operator_drop_index.hpp"

#include <components/cursor/cursor.hpp>
#include <components/index/disk/route.hpp>
#include <components/index/single_field_index.hpp>
#include <core/pmr.hpp>
#include <services/collection/collection.hpp>
#include <services/collection/session/session.hpp>
#include <services/disk/index_disk.hpp>

namespace services::collection::operators {

    using components::ql::create_index_t;
    using components::ql::index_type;

    using components::index::single_field_index_t;

    using namespace components::cursor;
    using namespace core::pmr;

    operator_drop_index::operator_drop_index(context_collection_t* context, components::ql::drop_index_t* ql)
        : read_write_operator_t(context, operator_type::drop_index)
        , ql_{ql} {}

    void operator_drop_index::on_execute_impl(components::pipeline::context_t* pipeline_context) {
        trace(context_->log(),
              "operator_drop_index::on_execute_impl session: {}, index: {}",
              pipeline_context->session.data(),
              ql_->name());

        auto index_ptr = components::index::search_index(context_->index_engine(), ql_->name());
        if (!index_ptr) {
            pipeline_context->send(pipeline_context->current_message_sender,
                                   collection::handler_id(collection::route::execute_plan_finish),
                                   make_cursor(context_->resource(), error_code_t::collection_not_exists));
            return;
        }

        if (index_ptr->is_disk()) {
            pipeline_context->send(context_->disk(),
                                   services::index::handler_id(index::route::drop),
                                   ql_->name(),
                                   context_);
        }
        components::index::drop_index(context_->index_engine(), index_ptr);
        pipeline_context->send(pipeline_context->current_message_sender,
                               collection::handler_id(collection::route::execute_plan_finish),
                               make_cursor(context_->resource(), operation_status_t::success));
    }
} // namespace services::collection::operators

#include "operator_add_index.hpp"
#include <components/cursor/cursor.hpp>
#include <components/index/disk/route.hpp>
#include <components/index/single_field_index.hpp>
#include <core/pmr.hpp>
#include <services/collection/collection.hpp>
#include <services/collection/create_index_utils.hpp>
#include <services/collection/route.hpp>
#include <services/collection/session/session.hpp>
#include <services/disk/index_disk.hpp>

namespace services::collection::operators {

    operator_add_index::operator_add_index(context_collection_t* context, components::ql::create_index_t* ql)
        : read_write_operator_t(context, operator_type::add_index)
        , index_ql_{ql} {}

    void operator_add_index::on_execute_impl(components::pipeline::context_t* pipeline_context) {
        trace(context_->log(),
              "operator_add_index::on_execute_impl session: {}, index: {}",
              pipeline_context->session.data(),
              index_ql_->name());

        // Index has pre setup compare type already
        if (index_ql_->index_compare_ != components::types::logical_type::UNKNOWN) {
            create_index_impl(context_, pipeline_context, std::move(index_ql_));
            return;
        }

        // If no documents exist and current index hasn't compare type, add to pending indexes vector
        if (context_->storage().empty()) {
            trace(context_->log(), "No documents found, add create_index to pending indexes");
            pipeline_context->send(
                pipeline_context->current_message_sender,
                services::collection::handler_id(services::collection::route::execute_plan_finish),
                components::cursor::make_cursor(context_->resource(), components::cursor::operation_status_t::success));
            context_->pending_indexes().emplace_back(
                pending_index_create{std::make_unique<components::ql::create_index_t>(std::move(*index_ql_.get())),
                                     std::make_unique<components::pipeline::context_t>(std::move(*pipeline_context))});
            index_ql_.release();
            return;
        }

        if (!try_update_index_compare(context_->storage().begin()->second, index_ql_)) {
            warn(context_->log(),
                 "Can't deduce compare type for index: {} with key {}",
                 index_ql_->name_,
                 index_ql_->keys_.front().as_string());
            pipeline_context->send(
                pipeline_context->current_message_sender,
                services::collection::handler_id(services::collection::route::execute_plan_finish),
                components::cursor::make_cursor(context_->resource(),
                                                components::cursor::error_code_t::index_create_fail,
                                                "index with name : " + index_ql_->name_ + " fail to deduce type"));
            return;
        }
        create_index_impl(context_, pipeline_context, std::move(index_ql_));
    }
} // namespace services::collection::operators

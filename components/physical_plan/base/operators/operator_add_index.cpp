#include "operator_add_index.hpp"
#include <components/cursor/cursor.hpp>
#include <components/index/disk/route.hpp>
#include <components/index/single_field_index.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <core/pmr.hpp>
#include <services/collection/collection.hpp>
#include <services/collection/route.hpp>
#include <services/collection/session/session.hpp>
#include <services/disk/index_disk.hpp>

namespace components::base::operators {

    operator_add_index::operator_add_index(services::collection::context_collection_t* context,
                                           logical_plan::node_create_index_ptr node)
        : read_write_operator_t(context, operator_type::add_index)
        , index_node_{std::move(node)} {}

    void operator_add_index::on_execute_impl(pipeline::context_t* pipeline_context) {
        trace(context_->log(),
              "operator_add_index::on_execute_impl session: {}, index: {}",
              pipeline_context->session.data(),
              index_node_->name());
        switch (index_node_->type()) {
            case logical_plan::index_type::single: {
                const bool index_exist = context_->index_engine()->has_index(index_node_->name());
                const auto id_index = index_exist
                                          ? index::INDEX_ID_UNDEFINED
                                          : index::make_index<index::single_field_index_t>(context_->index_engine(),
                                                                                           index_node_->name(),
                                                                                           index_node_->keys());

                services::collection::sessions::make_session(
                    context_->sessions(),
                    pipeline_context->session,
                    index_node_->name(),
                    services::collection::sessions::create_index_t{pipeline_context->current_message_sender, id_index});
                pipeline_context->send(context_->disk(),
                                       services::index::handler_id(services::index::route::create),
                                       std::move(index_node_),
                                       context_);
                break;
            }
            case logical_plan::index_type::composite:
            case logical_plan::index_type::multikey:
            case logical_plan::index_type::hashed:
            case logical_plan::index_type::wildcard: {
                trace(context_->log(), "index_type not implemented");
                assert(false && "index_type not implemented");
                break;
            }
            case logical_plan::index_type::no_valid: {
                trace(context_->log(), "index_type not valid");
                assert(false && "index_type not valid");
                break;
            }
        }
    }

} // namespace components::base::operators

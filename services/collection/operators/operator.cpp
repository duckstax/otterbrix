#include "operator.hpp"
#include <services/collection/collection.hpp>

namespace services::collection::operators {

    bool is_success(const operator_t::ptr &op) {
        return !op || op->is_executed();
    }

    operator_t::operator_t(context_collection_t* context, operator_type type)
        : context_(context)
        , type_(type) {
    }

    void operator_t::on_execute(components::pipeline::context_t* pipeline_context) {
        if (state_ == operator_state::created || state_ == operator_state::running) {
            on_prepare_impl();
            state_ = operator_state::running;
            if (left_) {
                left_->on_execute(pipeline_context);
            }
            if (right_ && is_success(left_)) {
                right_->on_execute(pipeline_context);
            }
            if (is_success(left_) && is_success(right_)) {
                on_execute_impl(pipeline_context);
                if (!is_wait_sync_disk()) {
                    state_ = operator_state::executed;
                }
            }
        } else if (is_wait_sync_disk()) {
            on_resume_impl(pipeline_context);
            state_ = operator_state::executed;
        }
    }

    void operator_t::on_resume(components::pipeline::context_t* pipeline_context) {
        on_execute(pipeline_context);
    }

    void operator_t::async_wait() {
        state_ = operator_state::waiting;
    }

    bool operator_t::is_executed() const {
        return state_ == operator_state::executed;
    }

    bool operator_t::is_wait_sync_disk() const {
        return state_ == operator_state::waiting;
    }

    operator_state operator_t::state() const {
        return state_;
    }

    const operator_data_ptr& operator_t::output() const {
        return output_;
    }

    const operator_write_data_ptr& operator_t::modified() const {
        return modified_;
    }

    const operator_write_data_ptr& operator_t::no_modified() const {
        return no_modified_;
    }

    void operator_t::set_children(ptr left, ptr right) {
        left_ = std::move(left);
        right_ = std::move(right);
    }

    void operator_t::take_output(ptr &src) {
        output_ = std::move(src->output_);
    }

    void operator_t::clear() {
        state_ = operator_state::created;
        left_ = nullptr;
        right_ = nullptr;
        output_ = nullptr;
    }

    void operator_t::on_resume_impl(components::pipeline::context_t*) {
    }

    void operator_t::on_prepare_impl() {
    }


    read_only_operator_t::read_only_operator_t(context_collection_t* collection, operator_type type)
        : operator_t(collection, type) {
    }


    read_write_operator_t::read_write_operator_t(context_collection_t* collection, operator_type type)
        : operator_t(collection, type)
        , state_(read_write_operator_state::pending) {
    }

    ::document::wrapper_value_t get_value_from_document(const components::document::document_ptr &doc, const components::expressions::key_t &key) {
        return ::document::wrapper_value_t(components::document::document_view_t(doc).get_value(key.as_string()));
    }

} // namespace services::collection::operators

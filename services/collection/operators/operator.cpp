#include "operator.hpp"

namespace services::collection::operators {

    operator_t::operator_t(context_collection_t* context, operator_type type)
        : context_(context)
        , type_(type) {
    }

    void operator_t::on_execute(planner::transaction_context_t* transaction_context) {
        if (state_ == operator_state::created) {
            state_ = operator_state::running;
            if (left_) {
                left_->on_execute(transaction_context);
            }
            if (right_) {
                right_->on_execute(transaction_context);
            }
            on_execute_impl(transaction_context);
            state_ = operator_state::executed;
        }
    }

    operator_state operator_t::state() const {
        return state_;
    }

    const operator_data_ptr& operator_t::output() const {
        return output_;
    }

    void operator_t::set_children(ptr left, ptr right) {
        left_ = std::move(left);
        right_ = std::move(right);
    }


    read_only_operator_t::read_only_operator_t(context_collection_t* collection, operator_type type)
        : operator_t(collection, type) {
    }


    read_write_operator_t::read_write_operator_t(context_collection_t* collection, operator_type type)
        : operator_t(collection, type)
        , state_(read_write_operator_state::pending) {
    }

} // namespace services::collection::operators

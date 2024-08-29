#include "connector.hpp"

#include <utility>

namespace otterbrix {

    connector_t::connector_t(boost::intrusive_ptr<otterbrix_t> instance)
        : instance_(std::move(instance)) {}

    components::cursor::cursor_t_ptr connector_t::execute(const std::string& query) {
        assert(instance_);
        auto session = session_id_t();
        cursor_store_ = std::move(instance_->dispatcher()->execute_sql(session, query));
        return cursor_store_;
    }

    components::cursor::cursor_t_ptr connector_t::cursor() { return cursor_store_; }

    void connector_t::close() {
        instance_ = nullptr;
        cursor_store_ = nullptr;
    }

} // namespace otterbrix
#pragma once

#include <collection/operators/operator.hpp>
#include <services/collection/collection.hpp>

namespace services::collection::operators {

    class aggregation final {
    public:
        aggregation() = delete;
        aggregation(const aggregation&) = delete;
        aggregation& operator=(const aggregation&) = delete;
        ~aggregation() = delete;
        aggregation(context_collection_t* ptr);

        void on_execute(components::ql::find_statement& cond, components::cursor::sub_cursor_t* sub_cursor);

    private:
        context_collection_t* context_;
    };

} // namespace services::collection::operators
#pragma once

#include "services/collection/collection.hpp"

namespace services::collection::operators {

    class full_scan final {
    public:
        full_scan(collection_t* collection);

        void on_execute(components::ql::find_one_statement& cond, components::cursor::sub_cursor_t* sub_cursor);

        collection_t* collection_;
    };

} // namespace services::operators
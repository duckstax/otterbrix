#include "full_scan.hpp"

namespace services::collection::operators {

    full_scan::full_scan(services::collection::collection_t* collection)
        : collection_(collection) {
    }
    void full_scan::on_execute(components::ql::find_one_statement& cond, components::cursor::sub_cursor_t* sub_cursor) {
        for (auto& it : storage_) {
            if (cond->is_fit(it.second)) {
                return result_find_one(document_view_t(it.second));
            }
        }
    }

} // namespace services::collection::operators

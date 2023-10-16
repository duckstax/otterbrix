#pragma once

#include <core/handler_by_id.hpp>

namespace services::collection {

    enum class route : uint64_t {
        create_documents,
        execute_plan,
        insert_documents,
        delete_documents,
        update_documents,
        size,
        close_cursor,
        drop_collection,
        create_index,
        drop_index,

        create_documents_finish,
        execute_plan_finish,
        insert_finish,
        delete_finish,
        update_finish,
        size_finish,
        drop_collection_finish,
        create_index_finish,
        drop_index_finish
    };

    constexpr auto handler_id(route type) {
        return handler_id(group_id_t::collection, type);
    }

} // namespace services::collection

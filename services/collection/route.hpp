#pragma once

#include <core/handler_by_id.hpp>

namespace services::collection {

    enum class route : uint64_t {
        create_documents,
        insert,
        find,
        find_one,
        delete_one,
        delete_many,
        update_one,
        update_many,
        size,
        close_cursor,
        drop_collection,
        create_index,
        drop_index,

        create_documents_finish,
        insert_finish,
        find_finish,
        find_one_finish,
        delete_finish,
        update_finish,
        size_finish,
        drop_collection_finish,
        create_index_finish,
        drop_index_finish
    };

    constexpr uint64_t handler_id(route type) {
        return handler_id(group_id_t::collection, type);
    }

} // namespace services::collection

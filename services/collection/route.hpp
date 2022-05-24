#pragma once
#include <core/handler_by_id.hpp>
namespace services::collection {
    enum class route : uint64_t {
        create_documents,
        insert_one,
        insert_many,
        find,
        find_one,
        delete_one,
        delete_many,
        update_one,
        update_many,
        size,
        close_cursor,
        drop_collection,

        create_documents_finish,
        insert_one_finish,
        insert_many_finish,
        find_finish,
        find_one_finish,
        delete_finish,
        update_finish,
        size_finish,
        drop_collection_finish,
    };

    inline uint64_t handler_id(route type) {
        return handler_id(group_id_t::collection, type);
    }
} // namespace services::collection

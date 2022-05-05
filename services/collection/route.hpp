#pragma once

namespace services::collection {
    enum class route : uint64_t {
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

        insert_one_finish,
        insert_many_finish,
        find_finish,
        find_one_finish,
        delete_finish,
        update_finish,
        size_finish,
        drop_collection_finish,
    };
} // namespace services::collection

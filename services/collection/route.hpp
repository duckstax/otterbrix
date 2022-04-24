#pragma once

namespace services::collection::route {

    static constexpr auto insert_one = "collection::insert_one";
    static constexpr auto insert_many = "collection::insert_many";
    static constexpr auto find = "collection::find";
    static constexpr auto find_one = "collection::find_one";
    static constexpr auto delete_one = "collection::delete_one";
    static constexpr auto delete_many = "collection::delete_many";
    static constexpr auto update_one = "collection::update_one";
    static constexpr auto update_many = "collection::update_many";
    static constexpr auto size = "collection::size";
    static constexpr auto close_cursor = "collection::close_cursor";
    static constexpr auto drop_collection = "collection::drop_collection";

    static constexpr auto insert_one_finish = "collection::insert_one_finish";
    static constexpr auto insert_many_finish = "collection::insert_many_finish";
    static constexpr auto find_finish = "collection::find_finish";
    static constexpr auto find_one_finish = "collection::find_one_finish";
    static constexpr auto delete_finish = "collection::delete_finish";
    static constexpr auto update_finish = "collection::update_finish";
    static constexpr auto size_finish = "collection::size_finish";
    static constexpr auto drop_collection_finish = "collection::drop_collection_finish";

} // namespace services::collection::route

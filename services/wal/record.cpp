#include "record.hpp"
#include <components/logical_plan/forward.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_database.hpp>

namespace services::wal {
    using namespace components::logical_plan;

    bool record_t::is_valid() const { return size > 0; }

    void record_t::set_data(msgpack::object& object, std::pmr::memory_resource* resource) {
        using namespace components::logical_plan;
        switch (type) {
            case node_type::insert_t:
                data = to_node_insert(object.via.array.ptr[0], resource);
                params = nullptr;
                break;
            case node_type::delete_t:
                data = to_node_delete(object.via.array.ptr[0], resource);
                params = to_storage_parameters(object.via.array.ptr[1], resource);
                break;
            case node_type::update_t:
                data = to_node_update(object, resource);
                params = to_storage_parameters(object.via.array.ptr[1], resource);
                break;
            default:
                data = to_node_default(object.via.array.ptr[0], type, resource);
                params = nullptr;
                break;
        }
    }

} // namespace services::wal

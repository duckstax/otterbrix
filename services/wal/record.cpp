#include "record.hpp"
#include <components/logical_plan/forward.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_database.hpp>

namespace services::wal {
    using namespace components::logical_plan;

    bool record_t::is_valid() const { return size > 0; }

    void
    record_t::set_data(msgpack::object& node_obj, msgpack::object& params_obj, std::pmr::memory_resource* resource) {
        using namespace components::logical_plan;
        switch (type) {
            case node_type::insert_t:
                data = to_node_insert(node_obj, resource);
                break;
            case node_type::delete_t:
                data = to_node_delete(node_obj, resource);
                break;
            case node_type::update_t:
                data = to_node_update(node_obj, resource);
                break;
            default:
                data = to_node_default(node_obj, type, resource);
                break;
        }
        params = to_storage_parameters(params_obj, resource);
    }

} // namespace services::wal

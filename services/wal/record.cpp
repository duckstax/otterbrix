#include "record.hpp"
#include <components/ql/statements.hpp>

namespace services::wal {

    bool record_t::is_valid() const { return size > 0; }

    void record_t::set_data(msgpack::object& object, std::pmr::memory_resource* resource) {
        using namespace components::ql;
        switch (type) {
            case statement_type::create_database:
                data = object.as<create_database_t>();
                break;
            case statement_type::drop_database:
                data = object.as<drop_database_t>();
                break;
            case statement_type::create_collection:
                data = object.as<create_collection_t>();
                break;
            case statement_type::drop_collection:
                data = object.as<drop_collection_t>();
                break;
            case statement_type::insert_one:
                data = to_insert_one(object, resource);
                break;
            case statement_type::insert_many:
                data = to_insert_many(object, resource);
                break;
            case statement_type::delete_one:
                data = to_delete_one(object, resource);
                break;
            case statement_type::delete_many:
                data = to_delete_many(object, resource);
                break;
            case statement_type::update_one:
                data = to_update_one(object, resource);
                break;
            case statement_type::update_many:
                data = to_update_many(object, resource);
                break;
            default:
                break;
        }
    }

} // namespace services::wal

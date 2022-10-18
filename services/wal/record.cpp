#include "record.hpp"
#include <components/ql/statements.hpp>

namespace services::wal {

    bool record_t::is_valid() const {
        return size > 0;
    }

    void record_t::set_data(msgpack::object& object) {
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
                data = object.as<insert_one_t>();
                break;
            case statement_type::insert_many:
                data = object.as<insert_many_t>();
                break;
            case statement_type::delete_one:
                data = object.as<delete_one_t>();
                break;
            case statement_type::delete_many:
                data = object.as<delete_many_t>();
                break;
            case statement_type::update_one:
                data = object.as<update_one_t>();
                break;
            case statement_type::update_many:
                data = object.as<update_many_t>();
                break;
            default:
                break;
        }
    }

} // namespace services::wal

#pragma once

#include "create_database.hpp"
#include "drop_database.hpp"
#include "create_collection.hpp"
#include "drop_collection.hpp"
#include "insert_one.hpp"
#include "insert_many.hpp"
#include "delete_one.hpp"
#include "delete_many.hpp"
#include "update_one.hpp"
#include "update_many.hpp"

namespace components::protocol {

    using variant_statement_t = std::variant<
        components::protocol::create_database_t,
        components::protocol::drop_database_t,
        components::protocol::create_collection_t,
        components::protocol::drop_collection_t,
        insert_one_t,
        insert_many_t,
        delete_one_t,
        delete_many_t,
        update_one_t,
        update_many_t
        >;

} //namespace components::protocol

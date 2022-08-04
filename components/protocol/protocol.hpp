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
#include <components/ql/index.hpp>

namespace components::protocol {

    using variant_statement_t = std::variant<
        components::ql::create_database_t,
        components::ql::drop_database_t,
        components::ql::create_collection_t,
        components::ql::drop_collection_t,
        components::ql::insert_one_t,
        components::ql::insert_many_t,
        components::ql::delete_one_t,
        components::ql::delete_many_t,
        components::ql::update_one_t,
        components::ql::update_many_t,
        components::ql::create_index_t
        >;

} //namespace components::protocol

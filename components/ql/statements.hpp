#pragma once

#include "aggregate.hpp"
#include "index.hpp"
#include "join/join.hpp"
#include "statements/create_collection.hpp"
#include "statements/create_database.hpp"
#include "statements/delete_many.hpp"
#include "statements/delete_one.hpp"
#include "statements/drop_collection.hpp"
#include "statements/drop_database.hpp"
#include "statements/insert_many.hpp"
#include "statements/insert_one.hpp"
#include "statements/update_many.hpp"
#include "statements/update_one.hpp"
#include <variant>

namespace components::ql {

    using variant_statement_t = std::variant<unused_statement_t,
                                             create_database_t,
                                             drop_database_t,
                                             create_collection_t,
                                             drop_collection_t,
                                             insert_one_t,
                                             insert_many_t,
                                             aggregate_statement,
                                             delete_one_t,
                                             delete_many_t,
                                             update_one_t,
                                             update_many_t,
                                             create_index_t,
                                             drop_index_t,
                                             join_t,
                                             ql_statement_t*>;

} //namespace components::ql

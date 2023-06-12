#pragma once

#include <actor-zeta.hpp>
#include <components/ql/ql_statement.hpp>

namespace services::database {

    struct database_create_result final {
        database_create_result(bool created,
                               components::ql::ql_statement_t* statement,
                               actor_zeta::address_t address)
            : created_(created)
            , statement_(statement)
            , address_(address) {}

        bool created_;
        components::ql::ql_statement_t* statement_;
        actor_zeta::address_t address_;
    };

    struct collection_create_result final {
        collection_create_result(bool created,
                                 components::ql::ql_statement_t* statement,
                                 actor_zeta::address_t address)
            : created_(created)
            , statement_(statement)
            , address_(address) {}

        bool created_;
        components::ql::ql_statement_t* statement_;
        actor_zeta::address_t address_;
    };

} // namespace services::storage

#pragma once

#include <services/collection/route.hpp>
#include <services/database/route.hpp>
#include <services/disk/route.hpp>
#include <services/dispatcher/route.hpp>
#include <services/wal/route.hpp>

namespace duck_charmer {

    namespace dispatcher {
        using services::dispatcher::route;
        using services::dispatcher::handler_id;
    }

    namespace database {
        using services::database::route;
        using services::database::handler_id;
    }

    namespace collection {
        using services::collection::route;
        using services::collection::handler_id;
    }

    namespace disk {
        using services::disk::route;
        using services::disk::handler_id;
    }

    namespace wal {
        using services::wal::route;
        using services::wal::handler_id;
    }

} // namespace duck_charmer

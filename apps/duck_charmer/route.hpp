#pragma once

#include <services/collection/route.hpp>
#include <services/database/route.hpp>
#include <services/disk/route.hpp>
#include <services/dispatcher/route.hpp>
#include <services/wal/route.hpp>

namespace duck_charmer {

    namespace dispatcher {
        using services::dispatcher::route;
    }

    namespace database {
        using services::database::route;
    }

    namespace collection {
        using services::collection::route;
    }

    namespace disk {
        using services::disk::route;
    }

    namespace wal {
        using services::wal::route;
    }

} // namespace duck_charmer

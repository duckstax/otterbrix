#pragma once

#include <services/dispatcher/route.hpp>
#include <services/database/route.hpp>
#include <services/collection/route.hpp>
#include <services/disk/route.hpp>
#include <services/wal/route.hpp>

namespace duck_charmer {

    namespace dispatcher {
        using namespace services::dispatcher::route;
    }

    namespace database {
        using namespace services::database::route;
    }

    namespace collection {
        using namespace services::collection::route;
    }

    namespace disk {
        using namespace services::disk::route;
    }

    namespace wal {
        using namespace services::wal::route;
    }

} // namespace duck_charmer

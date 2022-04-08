#pragma once

#include <services/collection/route.hpp>
#include <services/database/route.hpp>
#include <services/disk/route.hpp>
#include <services/wal/route.hpp>

namespace duck_charmer {

    namespace manager_database {
       using namespace services::storage::manager_database;
    }

    namespace database {
        using namespace services::storage::database;
    }

    namespace collection {
        using namespace services::storage::collection;
    }

    namespace disk {
        using namespace services::disk::route;
    }

    namespace wal {
        using namespace services::wal::route;
    }

} // namespace kv

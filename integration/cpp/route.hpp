#pragma once

#include <services/collection/route.hpp>
#include <services/memory_storage/route.hpp>
#include <services/disk/route.hpp>
#include <services/dispatcher/route.hpp>
#include <services/wal/route.hpp>

namespace otterbrix {

    namespace dispatcher {
        using services::dispatcher::route;
        using services::dispatcher::handler_id;
    }

    namespace memory_storage {
        using services::memory_storage::route;
        using services::memory_storage::handler_id;
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

} // namespace python

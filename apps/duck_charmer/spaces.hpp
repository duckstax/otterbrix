#pragma once
#include <goblin-engineer/core.hpp>

#include "services/storage/collection.hpp"
#include "services/storage/database.hpp"

#include "components/log/log.hpp"

#include "dispatcher.hpp"

class PYBIND11_EXPORT spaces final {
public:
    spaces(spaces& other) = delete;

    void operator=(const spaces&) = delete;

    static spaces* get_instance();
    /*
    void SomeBusinessLogic() {
        // ...
    }

    std::string value() const {
        return value_;
    }
*/

    goblin_engineer::actor_address dispatcher() {
        return goblin_engineer::actor_address(dispatcher_);
    }

protected:
    spaces();

    static spaces* instance_;

    services::storage::manager_database_ptr manager_database_;
    services::storage::database_ptr database_;
    goblin_engineer::actor_address collection_;
    manager_dispatcher_ptr manager_dispatcher_;
    goblin_engineer::actor_address dispatcher_;
};
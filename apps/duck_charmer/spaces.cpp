#include "spaces.hpp"
#include <goblin-engineer/core.hpp>
spaces::spaces() {
    std::string log_dir("/tmp/docker_logs/");
    auto log = initialization_logger("duck_charmer", log_dir);
    log.set_level(log_t::level::trace);

    manager_database_ = goblin_engineer::make_manager_service<services::storage::manager_database_t>(log, 1, 1000);
    database_ = goblin_engineer::make_manager_service<services::storage::database_t>(manager_database_, log, 1, 1000);
    collection_ = goblin_engineer::make_service<services::storage::collection_t>(database_, log);

    manager_dispatcher_ = goblin_engineer::make_manager_service<manager_dispatcher_t>(log, 1, 1000);
    dispatcher_ = goblin_engineer::make_service<dispatcher_t>(manager_dispatcher_, log);
}
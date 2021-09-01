#include "spaces.hpp"
#include <goblin-engineer/core.hpp>

goblin_engineer::actor_address spaces::dispatcher() {
    return goblin_engineer::actor_address(dispatcher_);
}

spaces::spaces() {
    std::string log_dir("/tmp/docker_logs/");
    log_ = initialization_logger("duck_charmer", log_dir);
    log_.set_level(log_t::level::trace);
    log_.debug("spaces::spaces()");
    manager_database_ = goblin_engineer::make_manager_service<services::storage::manager_database_t>(log_, 1, 1000);
    database_ = goblin_engineer::make_manager_service<services::storage::database_t>(manager_database_, log_, 1, 1000);
    collection_ = goblin_engineer::make_service<services::storage::collection_t>(database_, log_);

    manager_dispatcher_ = goblin_engineer::make_manager_service<manager_dispatcher_t>(log_, 1, 1000);
    dispatcher_ = goblin_engineer::make_service<dispatcher_t>(manager_dispatcher_, log_);

    goblin_engineer::link(*manager_database_,dispatcher_);
    goblin_engineer::link(*database_,dispatcher_);
    goblin_engineer::link(collection_,dispatcher_);

    ///TODO: hack
    auto tmp = manager_database_->address();
    goblin_engineer::link(*database_,tmp);

}

log_t& spaces::get_log() {
    return log_;
}
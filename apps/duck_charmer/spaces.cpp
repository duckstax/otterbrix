#include "spaces.hpp"
#include <goblin-engineer/core.hpp>

goblin_engineer::address_t spaces::dispatcher() {
    return goblin_engineer::address_t(dispatcher_);
}

spaces::spaces() {
    std::string log_dir("/tmp/docker_logs/");
    log_ = initialization_logger("duck_charmer", log_dir);
    log_.set_level(log_t::level::trace);
    log_.debug("spaces::spaces()");
    manager_database_ = goblin_engineer::make_manager_service<services::storage::manager_database_t>(log_, 1, 1000);
    manager_dispatcher_ = goblin_engineer::make_manager_service<manager_dispatcher_t>(log_, 1, 1000);
    goblin_engineer::link(manager_database_,manager_dispatcher_);

}

log_t& spaces::get_log() {
    return log_;
}
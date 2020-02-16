#include "log.hpp"

namespace rocketjoe {

    log_t::log_t(std::shared_ptr<spdlog::async_logger> logger) : logger_(logger) {}

    auto log_t::clone() noexcept -> log_t {
        return logger_;
    }

    auto log_t::context(std::shared_ptr<spdlog::async_logger> logger) noexcept -> void {
        logger_= logger;
    }

}
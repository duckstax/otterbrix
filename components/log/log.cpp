#include "log.hpp"

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <boost/process/environment.hpp>
#include <spdlog/async.h>
#include <utility>

namespace components {

    constexpr static const char* __default__ = "__default__";

    log_t::log_t(std::shared_ptr<spdlog::async_logger> logger)
        : logger_(std::move(logger)) {}

    log_t::log_t(std::shared_ptr<spdlog::logger> logger)
        : logger_(std::move(logger)) {}

    auto log_t::clone() noexcept -> log_t {
        return logger_;
    }

    auto log_t::context(std::shared_ptr<spdlog::async_logger> logger) noexcept -> void {
        logger_ = std::move(logger);
    }

    auto initialization_logger() -> log_t {
        auto name = fmt::format("logs/{0}.txt", boost::this_process::get_id());
        spdlog::init_thread_pool(8192, 1);
        auto stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(name, true);
        std::vector<spdlog::sink_ptr> sinks{stdout_sink, file_sink};
        auto logger = std::make_shared<spdlog::async_logger>(
            __default__,
            sinks.begin(), sinks.end(),
            spdlog::thread_pool(),
            spdlog::async_overflow_policy::block);

        spdlog::flush_every(std::chrono::seconds(1)); //todo: hack
        logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%l] [pid %P tid %t] %v");
        spdlog::set_default_logger(logger); /// spdlog::register_logger(logger);
        return logger;
    }

    auto get_logger(const std::string& name) -> log_t {
        return spdlog::get(name);
    }

    auto get_logger() -> log_t {
        return spdlog::get(__default__);
    }

    auto initialization_logger(std::shared_ptr<spdlog::logger> logger) -> void {
        spdlog::register_logger(std::move(logger));
    }

} // namespace rocketjoe

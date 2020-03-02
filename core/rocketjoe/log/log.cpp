#include "log.hpp"

#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/rotating_file_sink.h>

#include <utility>
#include <spdlog/async.h>


namespace rocketjoe {

    constexpr  static char*  __default__ = "__default__";

    log_t::log_t(std::shared_ptr<spdlog::async_logger> logger) : logger_(std::move(logger)) {}

    log_t::log_t(std::shared_ptr<spdlog::logger> logger): logger_(std::move(logger)) {}

    auto log_t::clone() noexcept -> log_t {
        return logger_;
    }

    auto log_t::context(std::shared_ptr<spdlog::async_logger> logger) noexcept -> void {
        logger_= std::move(logger);
    }

    auto initialization_logger() -> void{
        spdlog::set_pattern("[%H:%M:%S %z] [%^%L%$] [thread %t] %v");

        spdlog::init_thread_pool(8192, 1);
        auto stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>("logs/log.txt", true);
        std::vector<spdlog::sink_ptr> sinks{stdout_sink, file_sink};
        auto logger = std::make_shared<spdlog::async_logger>(
                __default__,
                sinks.begin(),sinks.end(),
                spdlog::thread_pool(),
                spdlog::async_overflow_policy::block
        );

        spdlog::register_logger(logger);
    }

    auto get_logger(const std::string &name) -> log_t {
        return spdlog::get(name);
    }

    auto get_logger() -> log_t {
        return spdlog::get(__default__);
    }

    auto initialization_logger(std::shared_ptr<spdlog::logger> logger) -> void {
        spdlog::register_logger(std::move(logger));
    }

}
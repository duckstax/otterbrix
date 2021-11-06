#pragma once

#include <chrono>
#include <iostream>
#include <mutex>
#include <spdlog/async_logger.h>
#include <string>

#define GET_TRACE() fmt::format("{}:{}::{}", __FILE__, __LINE__, __func__)

//sets variable "prefix"
#define GET_PREFIX() [[maybe_unused]] const static auto prefix = fmt::format("{}::{}", typeid(this).name(), __func__);

//sets variable "prefix" with specified name
#define GET_PREFIX_N(name) [[maybe_unused]] const static auto prefix = fmt::format("{}::{}", name, __func__);

class log_t final {
public:
    enum class level {
        trace = spdlog::level::trace,
        debug = spdlog::level::debug,
        info = spdlog::level::info,
        warn = spdlog::level::warn,
        err = spdlog::level::err,
        critical = spdlog::level::critical,
        off = spdlog::level::off,
        level_nums
    };

    log_t() = default;

    log_t(std::shared_ptr<spdlog::async_logger>);
    log_t(std::shared_ptr<spdlog::logger>);
    ~log_t() = default;
    auto clone() noexcept -> log_t;
    auto set_level(level l) -> void;
    auto get_level() const -> log_t::level;
    auto context(std::shared_ptr<spdlog::async_logger> logger) noexcept -> void;

    template<typename MSGBuilder>
    auto trace(MSGBuilder&& msg_builder) noexcept -> void {
        logger_->trace(std::forward<MSGBuilder>(msg_builder));
    }

    template<typename MSGBuilder>
    auto info(MSGBuilder&& msg_builder) noexcept -> void {
        logger_->info(std::forward<MSGBuilder>(msg_builder));
    }

    template<typename MSGBuilder>
    auto warn(MSGBuilder&& msg_builder) noexcept -> void {
        logger_->warn(std::forward<MSGBuilder>(msg_builder));
    }

    template<typename MSGBuilder>
    auto error(MSGBuilder&& msg_builder) noexcept -> void {
        logger_->error(std::forward<MSGBuilder>(msg_builder));
    }

    template<typename MSGBuilder>
    auto debug(MSGBuilder&& msg_builder) noexcept -> void {
        logger_->debug(std::forward<MSGBuilder>(msg_builder));
    }

    template<typename MSGBuilder>
    auto critical(MSGBuilder&& msg_builder) noexcept -> void {
        logger_->critical(std::forward<MSGBuilder>(msg_builder));
    }

    template<typename S, typename... Args>
    auto trace(const S& format_str, Args&&... args) -> void {
        logger_->trace(fmt::format(format_str, std::forward<Args>(args)...));
    }

    template<typename S, typename... Args>
    auto info(const S& format_str, Args&&... args) -> void {
        logger_->info(fmt::format(format_str, std::forward<Args>(args)...));
    }

    template<typename S, typename... Args>
    auto debug(const S& format_str, Args&&... args) -> void {
        logger_->debug(fmt::format(format_str, std::forward<Args>(args)...));
    }

    template<typename S, typename... Args>
    auto warn(const S& format_str, Args&&... args) -> void {
        logger_->warn(fmt::format(format_str, std::forward<Args>(args)...));
    }

    template<typename S, typename... Args>
    auto error(const S& format_str, Args&&... args) -> void {
        logger_->error(fmt::format(format_str, std::forward<Args>(args)...));
    }

    template<typename S, typename... Args>
    auto critical(const S& format_str, Args&&... args) -> void {
        logger_->critical(fmt::format(format_str, std::forward<Args>(args)...));
    }

private:
    std::shared_ptr<spdlog::logger> logger_;
};

auto get_logger(const std::string&) -> log_t;
auto get_logger() -> log_t;
auto initialization_logger(std::shared_ptr<spdlog::logger>) -> void;
auto initialization_logger(const std::string& name, std::string prefix) -> log_t;
auto drop_logger(const std::string&) -> void;
auto drop_all_loggers() -> void;

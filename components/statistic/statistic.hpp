#pragma once

#include <chrono>
#include <list>
#include <memory>
#include <memory_resource>

#include <components/session/session.hpp>
#include <components/ql/ql_statement.hpp>

namespace components::statistic {

    using clock_t = std::chrono::steady_clock;
    using time_point = clock_t ::time_point;
    using duration_t = clock_t::duration;
    enum class trace_steps : char {
        unused = 0x00, // unused
        create_database,
        drop_database,
        create_collection,
        drop_collection,
        find,
        find_one,
        insert_one,
        insert_many,
        delete_one,
        delete_many,
        update_one,
        update_many,
        create_index
    };

    class trace_entry_t {
    public:
        trace_entry_t(ql::statement_type statement,session::session_id_t id)
            : statement_(statement)
            , id_(id) {

        }

        void start() {
            start_  = clock_t :: now();
        }

        void finish() {
            finish_  = clock_t::now();
        }

    private:
        time_point start_;
        time_point finish_;
        ql::statement_type statement_;
        session::session_id_t id_;

    };

    class trace_t final {
    public:
        trace_t(std::pmr::memory_resource*resource):trace_(resource){}

        void append(trace_entry_t entry) {
            trace_.push_back(std::move(entry));
        }

    private:
        std::pmr::list<trace_entry_t> trace_;
    };

    using trace_ptr = std::unique_ptr<trace_t>;

    class statistic_t final {
    public:
        statistic_t(std::pmr::memory_resource*resource):storage_(resource){}
        trace_ptr new_trace() {
            return std::make_unique<trace_t>(storage_.get_allocator().resource());
        }
    private:
        std::pmr::list<std::unique_ptr<trace_t>> storage_;
    };

    class stopwatch {
    public:
        stopwatch(trace_entry_t& entry)
            : entry_(entry) {
            entry_.start();
        }

        ~stopwatch() {
            entry_.finish();
        }

    private:
        trace_entry& entry_;
    };
} // namespace components::statistic
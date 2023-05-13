#pragma once

#include <deque>

#include "actor-zeta.hpp"
#include "clock_test.hpp"

namespace core::non_thread_scheduler {

    class scheduler_test_t final : public actor_zeta::scheduler_abstract_t {
    public:
        using super = actor_zeta::scheduler_abstract_t;

        scheduler_test_t(std::size_t num_worker_threads, std::size_t max_throughput);

        std::deque<actor_zeta::scheduler::resumable*> jobs;

        bool run_once();
        size_t run(size_t max_count = std::numeric_limits<size_t>::max());
        size_t advance_time(actor_zeta::clock::clock_t::duration_type);
        clock_test& clock() noexcept;

    protected:
        void start() override;
        void stop() override;
        void enqueue(actor_zeta::scheduler::resumable* ptr) override;

    private:
        clock_test clock_;
    };

} // namespace core::non_thread_scheduler

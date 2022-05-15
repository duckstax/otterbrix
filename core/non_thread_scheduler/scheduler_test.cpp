#include "scheduler_test.hpp"

namespace core::non_thread_scheduler {

    namespace {

        class dummy_worker : public actor_zeta::execution_unit {
        public:
            dummy_worker(scheduler_test_t* parent)
                : parent_(parent) {
            }

            void execute_later(actor_zeta::scheduler::resumable* ptr) override {
                parent_->jobs.push_back(ptr);
            }

        private:
            scheduler_test_t* parent_;
        };

    } // namespace

    scheduler_test_t::scheduler_test_t(std::size_t num_worker_threads,
                                       std::size_t max_throughput)
        : super(num_worker_threads, max_throughput) {
    }

    clock_test& scheduler_test_t::clock() noexcept {
        return clock_;
    }

    void scheduler_test_t::start() {}

    void scheduler_test_t::stop() {
        while (run() > 0) {
            clock_.trigger_timeouts();
        }
    }

    void scheduler_test_t::enqueue(actor_zeta::scheduler::resumable* ptr) {
        jobs.push_back(ptr);
    }

    bool scheduler_test_t::run_once() {
        if (jobs.empty()) {
            return false;
        }
        auto job = jobs.front();
        jobs.pop_front();
        dummy_worker worker{this};
        switch (job->resume(&worker, 1)) {
            case actor_zeta::scheduler::resume_result::resume:
                jobs.push_front(job);
                break;
            case actor_zeta::scheduler::resume_result::done:
            case actor_zeta::scheduler::resume_result::awaiting:
                intrusive_ptr_release(job);
                break;
            case actor_zeta::scheduler::resume_result::shutdown:
                break;
        }
        return true;
    }

    size_t scheduler_test_t::run(size_t max_count) {
        size_t res = 0;
        while (res < max_count && run_once()) {
            ++res;
        }
        return res;
    }

    size_t scheduler_test_t::advance_time(actor_zeta::clock::clock_t::duration_type time) {
        return clock_.advance_time(time);
    }
}

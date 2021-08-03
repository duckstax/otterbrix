#pragma once
#include <goblin-engineer/core.hpp>
namespace goblin_engineer{

    namespace detail {
        struct thread_pool_deleter final {
            void operator()(actor_zeta::abstract_executor* ptr) {
                ptr->stop();
                delete ptr;
            }
        };


    } // namespace detail

    using executor_ptr = std::unique_ptr<goblin_engineer::abstract_executor, detail::thread_pool_deleter>;
    using shared_work = actor_zeta::executor_t<actor_zeta::work_sharing>;
}
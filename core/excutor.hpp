#pragma once
#include <actor-zeta.hpp>

namespace actor_zeta {

    namespace detail {
        struct thread_pool_deleter final {
            void operator()(actor_zeta::scheduler_abstract_t* ptr) {
                ptr->stop();
                delete ptr;
            }
        };

    } // namespace detail

    using scheduler_ptr = std::unique_ptr<actor_zeta::scheduler_abstract_t, detail::thread_pool_deleter>;
    using shared_work = actor_zeta::scheduler_t<actor_zeta::work_sharing>;
    using scheduler_raw = actor_zeta::scheduler_abstract_t*;
} // namespace actor_zeta
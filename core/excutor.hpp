#pragma once

#include <actor-zeta.hpp>

namespace actor_zeta {

    using scheduler_ptr = std::unique_ptr<actor_zeta::scheduler_abstract_t>;
    using shared_work = actor_zeta::scheduler_t<actor_zeta::work_sharing>;
    using scheduler_raw = actor_zeta::scheduler_abstract_t*;

} // namespace actor_zeta

#pragma once

namespace services::collection::sessions {

    enum class type_t {
        create_index,
        suspend_plan,
    };

    template<typename T>
    struct session_base_t {
    public:
        type_t type() const { return T::type_impl(); }
    };

} // namespace services::collection::sessions

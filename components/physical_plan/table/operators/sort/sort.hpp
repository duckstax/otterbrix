#pragma once
#include <components/types/logical_value.hpp>
#include <functional>
#include <memory_resource>

namespace services::table::sort {

    using components::types::compare_t;

    enum class order
    {
        descending = -1,
        ascending = 1
    };

    class sorter_t {
        using function_t = std::function<compare_t(const std::pmr::vector<components::types::logical_value_t>&,
                                                   const std::pmr::vector<components::types::logical_value_t>&)>;

    public:
        explicit sorter_t() = default;
        explicit sorter_t(size_t index, order order_ = order::ascending);

        void add(size_t index, order order_ = order::ascending);
        bool operator()(const std::pmr::vector<components::types::logical_value_t>& vec1,
                        const std::pmr::vector<components::types::logical_value_t>& vec2) const {
            for (const auto& f : functions_) {
                auto res = f(vec1, vec2);
                if (res < compare_t::equals) {
                    return true;
                } else if (res > compare_t::equals) {
                    return false;
                }
            }
            return true;
        }

    private:
        std::vector<function_t> functions_;
    };

} // namespace services::table::sort

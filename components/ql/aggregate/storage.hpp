#pragma once

#include "forward.hpp"

#include <variant>

#include "group.hpp"
#include "match.hpp"
#include "sort.hpp"
#include "merge.hpp"

namespace components::ql::aggregate {

    class operator_storage_t final {
    public:
        operator_storage_t() = delete;

        template<class Target>
        operator_storage_t(Target&& target)
            : storage_(std::forward<Target>(target)) {
        }

        template<class Target>
        const Target& get() const {
            return std::get<Target>(storage_);
        }

    private:
        std::variant<
            aggregate::group_t,
            aggregate::match_t,
            aggregate::sort_t,
            aggregate::merge_t>
            storage_;
    };

} // namespace components::ql::aggregate
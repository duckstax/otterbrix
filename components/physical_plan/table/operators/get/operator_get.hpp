#pragma once

#include <components/physical_plan/base/operators/operator.hpp>

namespace components::table::operators::get {

    class operator_get_t : public boost::intrusive_ref_counter<operator_get_t> {
    public:
        types::logical_value_t value(const std::pmr::vector<types::logical_value_t>& row);

        operator_get_t(const operator_get_t&) = delete;
        operator_get_t& operator=(const operator_get_t&) = delete;
        virtual ~operator_get_t() = default;

    protected:
        operator_get_t() = default;

    private:
        virtual types::logical_value_t get_value_impl(const std::pmr::vector<types::logical_value_t>& row) = 0;
    };

    using operator_get_ptr = boost::intrusive_ptr<operator_get_t>;

} // namespace components::table::operators::get
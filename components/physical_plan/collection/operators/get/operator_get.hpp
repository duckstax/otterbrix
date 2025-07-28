#pragma once

#include <components/physical_plan/base/operators/operator.hpp>

namespace components::collection::operators::get {

    class operator_get_t : public boost::intrusive_ref_counter<operator_get_t> {
    public:
        document::value_t value(const document::document_ptr& document);

        operator_get_t(const operator_get_t&) = delete;
        operator_get_t& operator=(const operator_get_t&) = delete;
        virtual ~operator_get_t() = default;

    protected:
        operator_get_t() = default;

    private:
        virtual document::value_t get_value_impl(const document::document_ptr& document) = 0;
    };

    using operator_get_ptr = boost::intrusive_ptr<operator_get_t>;

} // namespace components::collection::operators::get
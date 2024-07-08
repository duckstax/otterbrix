#pragma once

#include <components/physical_plan/collection/operators/operator.hpp>

namespace services::collection::operators::get {

    class operator_get_t : public boost::intrusive_ref_counter<operator_get_t> {
    public:
        components::document::value_t value(const components::document::document_ptr& document,
                                            components::document::impl::base_document* tape);

        operator_get_t(const operator_get_t&) = delete;
        operator_get_t& operator=(const operator_get_t&) = delete;
        virtual ~operator_get_t() = default;

    protected:
        operator_get_t() = default;

    private:
        virtual components::document::value_t get_value_impl(const components::document::document_ptr& document,
                                                             components::document::impl::base_document* tape) = 0;
    };

    using operator_get_ptr = boost::intrusive_ptr<operator_get_t>;

} // namespace services::collection::operators::get
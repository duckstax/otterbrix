#pragma once

#include <services/collection/operators/operator.hpp>

namespace services::collection::operators::get {

    class operator_get_t {
    public:
        document::wrapper_value_t value(const components::document::document_ptr& document);

        operator_get_t(const operator_get_t&) = delete;
        operator_get_t& operator=(const operator_get_t&) = delete;
        virtual ~operator_get_t() = default;

    protected:
        operator_get_t() = default;

    private:
        virtual document::wrapper_value_t get_value_impl(const components::document::document_ptr& document) = 0;
    };

    using operator_get_ptr = std::unique_ptr<operator_get_t>;

} // namespace services::operators::get
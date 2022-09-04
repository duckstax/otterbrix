#pragma once

#include <services/collection/collection.hpp>


namespace services::collection::operators {

    class operator_t  {
    public:

        operator_t(collection_t* collection);
        virtual ~operator_t() = default;

        result_find scan(components::ql::find_statement& cond);

    protected:
        const context_collection_t* collection_;

    private:
        virtual result_find scan_impl(components::ql::find_statement& cond) = 0;
        virtual result_find scan_one_impl(components::ql::find_statement& cond) = 0;
    };

    using scanner_ptr = std::unique_ptr<scanner>;

} // namespace services::operators

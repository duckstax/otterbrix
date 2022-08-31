#pragma once

#include <services/collection/collection.hpp>
#include <services/collection/context.hpp>

namespace services::collection::operators {

    class scanner {
    public:
        scanner(const context_t &context, collection_t* collection);
        virtual ~scanner() = default;

        result_find scan(components::ql::find_statement& cond);

    protected:
        context_t context_;
        collection_t* collection_;

    private:
        virtual result_find scan_impl(components::ql::find_statement& cond) = 0;
        virtual result_find scan_one_impl(components::ql::find_statement& cond) = 0;
    };

    using scanner_ptr = std::unique_ptr<scanner>;

} // namespace services::operators

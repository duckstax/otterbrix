#pragma once
#include "document.hpp"
#include "document_view.hpp"

namespace components::storage {
    class conditional_expression {
    public:
        virtual ~conditional_expression() = default;
        virtual bool check(const document_view_t &) const = 0;
        virtual bool check(const document_t &) const = 0;
    };
}

#pragma once
#include "document.hpp"

namespace components::storage {
    class conditional_expression {
    public:
        virtual ~conditional_expression() = default;
        virtual bool check(document_t&) = 0;
    };
}
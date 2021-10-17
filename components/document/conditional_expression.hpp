#pragma once
#include "document.hpp"

namespace components::storage {
    class conditional_expression {
    public:
        virtual ~conditional_expression() = default;
        virtual bool check(const document_t &) const = 0;
    };
}

#pragma once
#include "document/document.hpp"
#include "document/document_view.hpp"
#include "find.hpp"

namespace components::ql {

    expr_ptr parse_find_condition(const document::document_view_t& condition);
    expr_ptr parse_find_condition(const document::document_ptr& condition);

} // namespace components::ql

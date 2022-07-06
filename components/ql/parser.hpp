#pragma once
#include "find.hpp"
#include "document/document.hpp"

namespace components::ql {

find_condition_ptr parse_find_condition(const document_view_t &condition);
find_condition_ptr parse_find_condition(const document_ptr &condition);

}

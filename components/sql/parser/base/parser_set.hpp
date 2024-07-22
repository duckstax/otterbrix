#pragma once

#include <components/document/document.hpp>
#include <components/sql/lexer/lexer.hpp>
#include <components/sql/parser/base/parser_result.hpp>
#include <memory_resource>

namespace components::sql::impl {

    parser_result parse_set(lexer_t& lexer, components::document::document_ptr& doc);

} // namespace components::sql::impl

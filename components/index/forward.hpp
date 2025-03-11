#pragma once

#include <vector>

#include <components/cursor/cursor.hpp>
#include <components/document/document.hpp>
#include <components/document/value.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_create_index.hpp>

namespace components::index {

    using components::logical_plan::index_type;
    using document_ptr = components::document::document_ptr;
    using doc_t = components::document::document_ptr;
    using key_t = expressions::key_t;
    using components::logical_plan::keys_base_storage_t;
    using id_index = uint32_t;
    using value_t = document::value_t;
    using query_t = expressions::compare_expression_ptr;
    using result_set_t = cursor::sub_cursor_t;

} // namespace components::index
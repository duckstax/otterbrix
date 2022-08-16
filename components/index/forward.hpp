#pragma once

#include <vector>

#include <components/document/document.hpp>
#include <components/document/wrapper_value.hpp>
#include <components/ql/expr.hpp>
#include <components/ql/index.hpp>
#include <components/cursor/cursor.hpp>

namespace components::index {

    using components::ql::index_type;
    using document_ptr = components::document::document_ptr;
    using doc_t = components::document::document_ptr;
    using key_t = ql::key_t;
    using keys_base_storage_t = std::pmr::vector<key_t>;
    using id_index = uint32_t;
    using value_t = ::document::wrapper_value_t;
    using query_t = ql::expr_t::ptr;
    using result_set_t = cursor::sub_cursor_t;

}

/*
#include <components/document/document.hpp>
#include <components/document/document_view.hpp>
#include <components/document/wrapper_value.hpp>
#include <components/log/log.hpp>
#include <components/parser/conditional_expression.hpp>
#include <components/ql/expr.hpp>
#include <components/session/session.hpp>
#include <components/cursor/cursor.hpp>

#include <actor-zeta/detail/pmr/memory_resource.hpp>
#include <actor-zeta/detail/pmr/polymorphic_allocator.hpp>
 */
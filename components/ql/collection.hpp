#pragma once

namespace components::ql {

    enum class join_t { inner, full, left, right, cross, natural };

    struct collection_ref_t {};

    struct join_definition_t {
        join_definition_t();
        virtual ~join_definition_t();

        collection_ref_t* left;
        collection_ref_t* right;
        expr_t* condition;

        join_t type;
    };

} // namespace components::ql
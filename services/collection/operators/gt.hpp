#pragma once

#include "predicate.hpp"

class gt : public predicate {
public:
    bool check(components::document::document_ptr document,
               const std::string &key,
               document::wrapper_value_t value) final;
};

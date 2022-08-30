#pragma once

#include <components/document/wrapper_value.hpp>
#include <components/document/document.hpp>

class predicate {
public:
    virtual bool check(components::document::document_ptr document,
                       const std::string &key,
                       document::wrapper_value_t value) = 0;

    virtual ~predicate() = default;

protected:
    static document::wrapper_value_t get_value(components::document::document_ptr document,
                                               const std::string &key);
};

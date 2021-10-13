#pragma once
#include <list>
#include "components/storage/document.hpp"

class result_find
{
public:
    result_find() = default;
    result_find(std::list<components::storage::document_t *> &&finded_docs);

    std::list<components::storage::document_t *> finded_docs_;
};


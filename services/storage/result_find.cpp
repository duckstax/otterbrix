#include "result_find.hpp"

result_find::result_find(std::list<components::storage::document_t *> &&finded_docs)
    : finded_docs_(std::move(finded_docs))
{}

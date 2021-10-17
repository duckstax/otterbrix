#pragma once
#include <vector>
#include "components/document/document.hpp"

class result_find {
public:
    result_find() = delete;
    result_find(std::vector<components::storage::document_t *> &&finded_docs);
    const std::vector<components::storage::document_t *> &operator *() const;

private:
    std::vector<components::storage::document_t *> finded_docs_;
};


class result_size {
public:
    result_size() = delete;
    result_size(std::size_t size);
    std::size_t operator *() const;

private:
    std::size_t size_;
};


#pragma once
#include <vector>
#include "components/document/document_view.hpp"

class result_find {
public:
    using result_t = std::vector<components::storage::document_view_t>;

    result_find() = delete;
    result_find(result_t &&finded_docs);
    const result_t &operator *() const;
    result_t *operator ->();

private:
    result_t finded_docs_;
};


class result_size {
public:
    result_size() = delete;
    result_size(std::size_t size);
    std::size_t operator *() const;

private:
    std::size_t size_;
};


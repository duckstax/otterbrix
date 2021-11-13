#pragma once
#include <vector>
#include "components/document/document_view.hpp"

class result_find {
public:
    using result_t = std::vector<components::document::document_view_t>;

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


class result_get_document {
public:
    using result_t = components::document::document_view_t;

    result_get_document() = delete;
    result_get_document(result_t &&doc);
    const result_t &operator *() const;
    result_t *operator ->();

private:
    result_t doc_;
};


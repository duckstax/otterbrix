#pragma once
#include <vector>
#include "components/document/document_view.hpp"

class result_insert_one {
public:
    using result_t = std::string;

    result_insert_one() = default;
    explicit result_insert_one(result_t id);
    const result_t &inserted_id() const;

private:
    result_t inserted_id_;
    //TODO: except or error_code
};


class result_insert_many {
public:
    using result_t = std::vector<std::string>;

    result_insert_many() = default;
    explicit result_insert_many(result_t &&inserted_ids);
    const result_t &inserted_ids() const;

private:
    result_t inserted_ids_;
    //TODO: except or error_code
};


class result_find {
public:
    using result_t = std::vector<components::document::document_view_t>;

    result_find() = default;
    explicit result_find(result_t &&finded_docs);
    const result_t &operator *() const;
    result_t *operator ->();

private:
    result_t finded_docs_;
};


class result_find_one {
public:
    using result_t = components::document::document_view_t;

    result_find_one() = default;
    explicit result_find_one(const result_t &finded_doc);
    bool is_find() const;
    const result_t &operator *() const;
    result_t *operator ->();

private:
    result_t finded_doc_;
    bool is_find_ {false};
};


class result_size {
public:
    using result_t = std::size_t;

    result_size() = default;
    explicit result_size(result_t size);
    result_t operator *() const;

private:
    result_t size_ {0};
};


class result_get_document {
public:
    using result_t = components::document::document_view_t;

    result_get_document() = default;
    explicit result_get_document(result_t &&doc);
    const result_t &operator *() const;
    result_t *operator ->();

private:
    result_t doc_;
};


class result_drop_collection {
public:
    using result_t = bool;

    result_drop_collection() = default;
    explicit result_drop_collection(result_t success);
    result_t is_success() const;

private:
    result_t success_ {false};
};

#pragma once
#include "components/document/document_view.hpp"
#include "components/document/document_id.hpp"
#include <vector>

class result_insert_one {
public:
    using result_t = components::document::document_id_t;

    result_insert_one();
    explicit result_insert_one(const result_t &id);
    const result_t& inserted_id() const;
    bool empty() const;

private:
    result_t inserted_id_;
    //TODO: except or error_code
};

class result_insert_many {
public:
    using result_t = std::vector<components::document::document_id_t>;

    result_insert_many() = default;
    explicit result_insert_many(result_t&& inserted_ids);
    const result_t& inserted_ids() const;
    bool empty() const;

private:
    result_t inserted_ids_;
    //TODO: except or error_code
};

class result_find {
public:
    using result_t = std::vector<components::document::document_view_t>;

    result_find() = default;
    explicit result_find(result_t&& finded_docs);
    const result_t& operator*() const;
    result_t* operator->();

private:
    result_t finded_docs_;
};

class result_find_one {
public:
    using result_t = components::document::document_view_t;

    result_find_one() = default;
    explicit result_find_one(const result_t& finded_doc);
    bool is_find() const;
    const result_t& operator*() const;
    result_t* operator->();

private:
    result_t finded_doc_;
    bool is_find_{false};
};

class result_size {
public:
    using result_t = std::size_t;

    result_size() = default;
    explicit result_size(result_t size);
    result_t operator*() const;

private:
    result_t size_{0};
};

class result_drop_collection {
public:
    using result_t = bool;

    result_drop_collection() = default;
    explicit result_drop_collection(result_t success);
    result_t is_success() const;

private:
    result_t success_{false};
};

class result_delete {
public:
    using result_t = std::vector<components::document::document_id_t>;

    result_delete() = default;
    explicit result_delete(result_t&& deleted_ids);
    const result_t& deleted_ids() const;
    bool empty() const;

private:
    result_t deleted_ids_;
    //TODO: except or error_code
};

class result_update {
public:
    using document_id_t = components::document::document_id_t;
    using result_t = std::vector<document_id_t>;

    result_update();
    result_update(result_t&& modified_ids, result_t&& nomodified_ids);
    explicit result_update(document_id_t&& upserted_id);
    const result_t& modified_ids() const;
    const result_t& nomodified_ids() const;
    const document_id_t& upserted_id() const;
    bool empty() const;

private:
    result_t modified_ids_;
    result_t nomodified_ids_;
    document_id_t upserted_id_;
    //TODO: except or error_code
};

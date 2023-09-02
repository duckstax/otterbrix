#pragma once

#include <memory_resource>
#include <variant>
#include <vector>
#include <components/cursor/cursor.hpp>
#include <components/document/document_view.hpp>
#include <components/document/document_id.hpp>

class null_result {
public:
    null_result() {}
};

class result_insert {
public:
    using result_t = std::pmr::vector<components::document::document_id_t>;

    explicit result_insert(std::pmr::memory_resource *resource);
    explicit result_insert(result_t&& inserted_ids);
    const result_t& inserted_ids() const;
    bool empty() const;

private:
    result_t inserted_ids_;
    //TODO: except or error_code
};


class result_find {
public:
    using result_t = std::pmr::vector<components::document::document_view_t>;

    explicit result_find(std::pmr::memory_resource *resource);
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
    explicit result_find_one(result_t&& finded_doc);
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
    using result_t = std::pmr::vector<components::document::document_id_t>;

    explicit result_delete(std::pmr::memory_resource *resource);
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
    using result_t = std::pmr::vector<document_id_t>;

    result_update() = default;
    explicit result_update(std::pmr::memory_resource *resource);
    result_update(result_t&& modified_ids, result_t&& nomodified_ids);
    result_update(const result_t& modified_ids, const result_t& nomodified_ids);
    result_update(const document_id_t& upserted_id, std::pmr::memory_resource *resource);
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


class result_create_index {
public:
    using result_t = bool;

    result_create_index() = default;
    explicit result_create_index(result_t success);
    result_t is_success() const;

private:
    result_t success_{false};
};


class result_drop_index {
public:
    using result_t = bool;

    result_drop_index() = default;
    explicit result_drop_index(result_t success);
    result_t is_success() const;

private:
    result_t success_{false};
};


class result_t {
public:
    template <typename T>
    result_t(const T& t)
        : result_(t) {}

    template <typename T>
    const T& get() const {
        return std::get<T>(result_);
    }

    template <typename T>
    bool is_type() const {
        return std::holds_alternative<T>(result_);
    }

    template <class Visitor>
    void visit(Visitor&&visitor) {
        std::visit(visitor, result_);
    }

private:
    std::variant<
        null_result,
        components::cursor::cursor_t*,
        result_insert,
        result_delete,
        result_update
    > result_;

};

#pragma once

#include <memory_resource>
#include <variant>
#include <actor-zeta.hpp>
#include <components/cursor/cursor.hpp>
#include <components/ql/base.hpp>

namespace components::result {

    enum class error_code_t {
        database_already_exists,
        database_not_exists,
        collection_already_exists,
        collection_not_exists,
        collection_dropped,
        sql_parse_error,
        create_phisical_plan_error,
        other_error
    };


    struct empty_result_t {
    };


    struct error_result_t {
        error_code_t code;
        std::string what;

        explicit error_result_t(error_code_t code, const std::string& what);
    };


    struct result_address_t {
        actor_zeta::address_t address{actor_zeta::address_t::empty_address()};

        result_address_t() = default;
        explicit result_address_t(actor_zeta::address_t address);
    };


    struct result_list_addresses_t {
        struct res_t {
            collection_full_name_t name;
            actor_zeta::address_t address;
        };

        std::pmr::vector<res_t> addresses;

        explicit result_list_addresses_t(std::pmr::memory_resource *resource);
    };


    struct result_insert {
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


    struct result_find {
    public:
        using result_t = std::pmr::vector<components::document::document_view_t>;

        explicit result_find(std::pmr::memory_resource *resource);
        explicit result_find(result_t&& finded_docs);
        const result_t& operator*() const;
        result_t* operator->();

    private:
        result_t finded_docs_;
    };


    struct result_find_one {
    public:
        using result_t = components::document::document_view_t;

        result_find_one() = default;
        explicit result_find_one(result_t finded_doc);
        bool is_find() const;
        const result_t& operator*() const;
        result_t* operator->();

    private:
        result_t finded_doc_;
        bool is_find_{false};
    };


    struct result_size {
    public:
        using result_t = std::size_t;

        result_size() = default;
        explicit result_size(result_t size);
        result_t operator*() const;

    private:
        result_t size_{0};
    };


    struct result_drop_collection {
    public:
        using result_t = bool;

        result_drop_collection() = default;
        explicit result_drop_collection(result_t success);
        result_t is_success() const;

    private:
        result_t success_{false};
    };


    struct result_delete {
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


    struct result_update {
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


    struct result_create_index {
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
        result_t();

        explicit result_t(error_result_t result);

        template <typename T>
        explicit result_t(T result)
            : result_(std::move(result))
            , is_error_(false) {
        }

        bool is_error() const;
        bool is_success() const;
        error_code_t error_code() const;
        const std::string& error_what() const;

        template <typename T>
        bool is_type() const {
            return std::holds_alternative<T>(result_);
        }

        template <typename T>
        const T& get() const {
            return std::get<T>(result_);
        }

        template<class F>
        void visit(F&&f){
            std::visit(f,result_);
        }

    private:
        std::variant<
            error_result_t,
            empty_result_t,
            result_address_t,
            result_list_addresses_t,
            components::cursor::cursor_t*,
            result_insert,
            result_delete,
            result_update
        > result_;
        bool is_error_;
    };


    template <typename TResult>
    result_t make_result(TResult result) {
        return result_t{std::move(result)};
    }

    result_t make_error(error_code_t code, const std::string& error);

} // namespace components::result

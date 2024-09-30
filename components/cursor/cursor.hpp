#pragma once

#include <vector>

#include <actor-zeta.hpp>

#include <components/base/collection_full_name.hpp>
#include <components/document/document.hpp>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set_hook.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

namespace components::cursor {

    using data_ptr = document::document_ptr;
    using index_t = int32_t;
    constexpr index_t start_index = -1;

    enum class operation_status_t
    {
        success = 1,
        failure = 0
    };

    enum class error_code_t : int32_t
    {
        other_error = -1,
        none = 0,
        database_already_exists = 1,
        database_not_exists = 2,
        collection_already_exists = 3,
        collection_not_exists = 4,
        index_create_fail = 5,
        collection_dropped = 6,
        sql_parse_error = 7,
        create_phisical_plan_error = 8
    };

    struct error_t {
        error_code_t type;
        std::string what;

        explicit error_t(error_code_t type);
        explicit error_t(error_code_t type, const std::string& what);
    };

    class sub_cursor_t {
    public:
        sub_cursor_t(std::pmr::memory_resource* resource, collection_full_name_t collection_name);
        const collection_full_name_t& collection_name();
        std::size_t size() const;
        std::pmr::vector<data_ptr>& data();
        void append(data_ptr);

    private:
        collection_full_name_t collection_name_;
        std::pmr::vector<data_ptr> data_;
    };

    class cursor_t : public boost::intrusive_ref_counter<cursor_t> {
    public:
        explicit cursor_t(std::pmr::memory_resource* resource);
        explicit cursor_t(std::pmr::memory_resource* resource, const error_t& error);
        explicit cursor_t(std::pmr::memory_resource* resource, operation_status_t op_status);
        void push(std::unique_ptr<sub_cursor_t> sub_cursor);
        std::size_t size() const;
        std::pmr::vector<std::unique_ptr<sub_cursor_t>>::iterator begin();
        std::pmr::vector<std::unique_ptr<sub_cursor_t>>::iterator end();
        bool has_next() const;
        data_ptr next();
        data_ptr get() const;
        data_ptr get(std::size_t index) const;
        bool is_success() const noexcept;
        bool is_error() const noexcept;
        error_t get_error() const;
        void sort(std::function<bool(data_ptr, data_ptr)> sorter);

    private:
        std::size_t size_{};
        index_t current_index_{start_index};
        std::pmr::vector<std::unique_ptr<sub_cursor_t>> sub_cursor_;
        std::pmr::vector<data_ptr> sorted_;
        error_t error_;
        bool success_{true};

        void create_list_by_sort();
        data_ptr get_sorted(std::size_t index) const;
        data_ptr get_unsorted(std::size_t index) const;
    };

    using cursor_t_ptr = boost::intrusive_ptr<cursor_t>;

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, operation_status_t op_status);
    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource);
    cursor_t_ptr
    make_cursor(std::pmr::memory_resource* resource, error_code_t type, const std::string& what = std::string());
} // namespace components::cursor

#pragma once
#include <memory>
#include <unordered_map>

#include <goblin-engineer/core.hpp>
#include <pybind11/pytypes.h>

#include "log/log.hpp"

#include "protocol/base.hpp"
#include "protocol/forward.hpp"

#include "storage/support/ref_counted.hpp"

#include "storage/conditional_expression.hpp"
#include "storage/document.hpp"
#include "storage/document_view.hpp"

#include "forward.hpp"
#include "route.hpp"
#include "query.hpp"
#include "result.hpp"

namespace storage::impl {
class mutable_dict_t;
class mutable_array_t;
class value_t;
}

class test_collection_t;

namespace services::storage {
    using document_t  = components::storage::document_t;
    using document_view_t = components::storage::document_view_t;

    class collection_t final : public goblin_engineer::abstract_service {
    public:
        using storage_t = std::stringstream;
        using index_t = ::storage::retained_t<::storage::impl::mutable_dict_t>;
        using field_index_t = ::storage::retained_t<::storage::impl::value_t>;
        using field_value_t = const ::storage::impl::value_t *;

        collection_t(database_ptr database, log_t& log);
        ~collection_t();

        void insert(session_t& session_t, std::string& collection, document_t &&document);
        auto get(components::storage::conditional_expression& cond) -> void;
        auto search(const session_t &session, const std::string &collection, query_ptr cond) -> void;
        auto find(const session_t& session, const std::string &collection, const document_t &cond) -> void;
        auto all() -> void;
       /// void insert_many(py::iterable iterable);
        auto size(session_t& session, std::string& collection) -> void;
        void update(document_t& fields, components::storage::conditional_expression& cond);
        void remove(components::storage::conditional_expression& cond);
        void drop();

    private:
        std::string gen_id() const;
        void insert_(document_t&& document, int version = 0);
        field_index_t insert_field_(field_value_t value, int version);
        document_view_t get_(const std::string& id) const;
        std::size_t size_() const;
        auto remove_(const std::string& key);
        void drop_();
        result_find search_(query_ptr cond);

        log_t log_;
        index_t index_;
        storage_t storage_;

#ifdef DEV_MODE
    public:
        void insert_test(document_t &&doc);
        result_find search_test(query_ptr cond);
        result_find find_test(const document_t &cond);
        std::string get_index_test() const;
        std::string get_data_test() const;
        std::size_t size_test() const;
        document_view_t get_test(const std::string &id) const;
#endif
    };

    using collection_ptr = goblin_engineer::intrusive_ptr<collection_t>;

} // namespace services::storage

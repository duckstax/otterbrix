#pragma once
#include <memory>
#include <unordered_map>

#include <goblin-engineer/core.hpp>
#include <pybind11/pytypes.h>

#include "log/log.hpp"

#include "protocol/base.hpp"
#include "protocol/forward.hpp"

#include "components/document/support/ref_counted.hpp"
#include "components/document/conditional_expression.hpp"
#include "components/document/document.hpp"
#include "components/document/document_view.hpp"
#include "components/cursor/cursor.hpp"
#include "components/session/session.hpp"

#include "forward.hpp"
#include "route.hpp"
#include "query.hpp"
#include "result.hpp"

namespace document::impl {
class mutable_dict_t;
class mutable_array_t;
class value_t;
}

namespace services::storage {

    using document_t  = components::document::document_t;
    using document_view_t = components::document::document_view_t;

    class collection_t final : public goblin_engineer::abstract_service {
    public:
        using storage_t = msgpack::sbuffer;
        using index_t = ::document::retained_t<::document::impl::mutable_dict_t>;
        using field_index_t = ::document::retained_t<::document::impl::value_t>;
        using field_value_t = const ::document::impl::value_t *;

        collection_t(database_ptr database, log_t& log);
        ~collection_t();

        void insert(session_t& session_t, std::string& collection, document_t &&document);
        auto get(components::document::conditional_expression& cond) -> void;
        auto search(const session_t &session, const std::string &collection, query_ptr cond) -> void;
        auto find(const session_t& session, const std::string &collection, const document_t &cond) -> void;
        auto all() -> void;
       /// void insert_many(py::iterable iterable);
        auto size(session_t& session, std::string& collection) -> void;
        void update(document_t& fields, components::document::conditional_expression& cond);
        void remove(components::document::conditional_expression& cond);
        void drop();
        void close_cursor(session_t& session);

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
        std::unordered_map<session_t,std::unique_ptr<components::cursor::data_cursor_t>> cursor_storage_;

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

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

        collection_t(goblin_engineer::supervisor_t*,std::string name, log_t& log);
        ~collection_t();

        auto size(session_t& session) -> void;
        void insert_one(session_t& session_t, document_t &document);
        void insert_many(session_t& session, std::list<document_t> &documents);
        auto find(const session_t& session, const document_t &cond) -> void;
        auto find_one(const session_t& session, const document_t &cond) -> void;
        auto delete_one(const session_t& session, const document_t &cond) -> void;
        auto delete_many(const session_t& session, const document_t &cond) -> void;
        void drop(const session_t& session);
        void close_cursor(session_t& session);

    private:
        std::string gen_id() const;
        std::string insert_(const document_t &document, int version = 0);
        field_index_t insert_field_(field_value_t value, int version);
        document_view_t get_(const std::string& id) const;
        std::size_t size_() const;
        bool drop_();
        result_find search_(query_ptr cond);
        result_find_one search_one_(query_ptr cond);
        result_delete delete_one_(query_ptr cond);
        result_delete delete_many_(query_ptr cond);
        void remove_(const std::string& id);

        log_t log_;
        goblin_engineer::address_t database_;
        index_t index_;
        storage_t storage_;
        std::unordered_map<session_t,std::unique_ptr<components::cursor::data_cursor_t>> cursor_storage_;
        bool dropped_ {false};

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

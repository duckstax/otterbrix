#pragma once
#include <memory>
#include <unordered_map>

#include <goblin-engineer/core.hpp>
#include <pybind11/pytypes.h>

#include "log/log.hpp"

#include "protocol/base.hpp"
#include "protocol/insert_many.hpp"

#include "components/cursor/cursor.hpp"
#include "components/btree_storage/btree_storage.hpp"
#include "components/document/document.hpp"
#include "components/document/document_view.hpp"
#include "components/document/support/ref_counted.hpp"
#include "components/parser/conditional_expression.hpp"
#include "components/session/session.hpp"

#include "forward.hpp"
#include "result.hpp"
#include "route.hpp"

namespace document::impl {
    class mutable_dict_t;
    class mutable_array_t;
    class value_t;
} // namespace document::impl

namespace services::storage {

    using storage_t = components::btree::storage_t;
    using document_id_t = components::document::document_id_t;
    using document_t = components::document::document_t;
    using document_view_t = components::document::document_view_t;
    using components::btree::make_document;
    using components::parser::find_condition_ptr;

    class collection_t final : public goblin_engineer::abstract_service {
    public:
        collection_t(goblin_engineer::supervisor_t*, std::string name, log_t& log);
        ~collection_t();
        auto size(session_id_t& session) -> void;
        void insert_one(session_id_t& session_t, document_t& document);
        void insert_many(session_id_t& session, std::list<document_t>& documents);
        auto find(const session_id_t& session, find_condition_ptr cond) -> void;
        auto find_one(const session_id_t& session, find_condition_ptr cond) -> void;
        auto delete_one(const session_id_t& session, find_condition_ptr cond) -> void;
        auto delete_many(const session_id_t& session, find_condition_ptr cond) -> void;
        auto update_one(const session_id_t& session, find_condition_ptr cond, const document_t& update, bool upsert) -> void;
        auto update_many(const session_id_t& session, find_condition_ptr cond, const document_t& update, bool upsert) -> void;
        void drop(const session_id_t& session);
        void close_cursor(session_id_t& session);

    private:
        document_id_t insert_(const document_t& document, int version = 0);
        document_view_t get_(const document_id_t& id) const;
        std::size_t size_() const;
        bool drop_();
        result_find search_(const find_condition_ptr& cond);
        result_find_one search_one_(const find_condition_ptr& cond);
        result_delete delete_one_(const find_condition_ptr& cond);
        result_delete delete_many_(const find_condition_ptr& cond);
        result_update update_one_(const find_condition_ptr& cond, const document_t& update, bool upsert);
        result_update update_many_(const find_condition_ptr& cond, const document_t& update, bool upsert);
        void remove_(const document_id_t& id);
        bool update_(const document_id_t& id, const document_t& update, bool is_commit);
        document_t update2insert(const document_t& update) const;

        log_t log_;
        goblin_engineer::address_t database_;
        storage_t storage_;
        std::unordered_map<session_id_t, std::unique_ptr<components::cursor::sub_cursor_t>> cursor_storage_;
        bool dropped_{false};

#ifdef DEV_MODE
    public:
        void insert_test(document_t&& doc);
        result_find find_test(find_condition_ptr cond);
        std::size_t size_test() const;
        document_view_t get_test(const std::string& id) const;
        result_delete delete_one_test(find_condition_ptr cond);
        result_delete delete_many_test(find_condition_ptr cond);
        result_update update_one_test(find_condition_ptr cond, const document_t& update, bool upsert);
        result_update update_many_test(find_condition_ptr cond, const document_t& update, bool upsert);
#endif
    };

    using collection_ptr = goblin_engineer::intrusive_ptr<collection_t>;

} // namespace services::storage

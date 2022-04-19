#include "collection.hpp"
#include <services/disk/route.hpp>

namespace services::storage {

    collection_t::collection_t(goblin_engineer::supervisor_t* database, std::string name, log_t& log, goblin_engineer::address_t mdisk)
        : goblin_engineer::abstract_service(database, std::move(name))
        , log_(log.clone())
        , database_(database->address())
        , mdisk_(mdisk) {
        add_handler(collection::insert_one, &collection_t::insert_one);
        add_handler(collection::insert_many, &collection_t::insert_many);
        add_handler(collection::find, &collection_t::find);
        add_handler(collection::find_one, &collection_t::find_one);
        add_handler(collection::delete_one, &collection_t::delete_one);
        add_handler(collection::delete_many, &collection_t::delete_many);
        add_handler(collection::update_one, &collection_t::update_one);
        add_handler(collection::update_many, &collection_t::update_many);
        add_handler(collection::size, &collection_t::size);
        add_handler(collection::drop_collection, &collection_t::drop);
        add_handler(collection::close_cursor, &collection_t::close_cursor);
    }

    auto collection_t::size(session_id_t& session) -> void {
        log_.debug("collection {}::size", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        auto result = dropped_
                      ? result_size()
                      : result_size(size_());
        goblin_engineer::send(dispatcher, address(), "size_finish", session, result);
    }


    void collection_t::insert_one(session_id_t& session, document_ptr& document) {
        log_.debug("collection_t::insert_one : {}", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        auto result = dropped_
                      ? result_insert_one()
                      : result_insert_one(insert_(document));
        if (!result.empty()) {
            std::vector<document_ptr> new_documents = {document};
            goblin_engineer::send(mdisk_, address(), disk::route::write_documents, session, std::string(database_.type()), std::string(type()), new_documents);
        }
        goblin_engineer::send(dispatcher, address(), "insert_one_finish", session, result);
    }

    void collection_t::insert_many(session_id_t& session, std::list<document_ptr> &documents) {
        log_.debug("collection_t::insert_many : {}", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        if (dropped_) {
            goblin_engineer::send(dispatcher, address(), "insert_many_finish", session, result_insert_many());
        } else {
            std::vector<document_ptr> new_documents;
            std::vector<document_id_t> result;
            for (const auto& document : documents) {
                auto id = insert_(document);
                if (!id.is_null()) {
                    result.emplace_back(std::move(id));
                    new_documents.push_back(document);
                }
            }
            if (!new_documents.empty()) {
                goblin_engineer::send(mdisk_, address(), disk::route::write_documents, session, std::string(database_.type()), std::string(type()), new_documents);
            }
            goblin_engineer::send(dispatcher, address(), "insert_many_finish", session, result_insert_many(std::move(result)));
        }
    }

    auto collection_t::find(const session_id_t& session, const find_condition_ptr& cond) -> void {
        log_.debug("collection::find : {}", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        if (dropped_) {
            goblin_engineer::send(dispatcher, address(), "find_finish", session, nullptr);
        } else {
            auto result = cursor_storage_.emplace(session, std::make_unique<components::cursor::sub_cursor_t>(address(), *search_(cond)));
            goblin_engineer::send(dispatcher, address(), "find_finish", session, result.first->second.get());
        }
    }


    void collection_t::find_one(const session_id_t& session, const find_condition_ptr& cond) {
        log_.debug("collection::find_one : {}", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        auto result = dropped_
                      ? result_find_one()
                      : search_one_(cond);
        goblin_engineer::send(dispatcher, address(), "find_one_finish", session, result);
    }

    auto collection_t::delete_one(const session_id_t& session, const find_condition_ptr& cond) -> void {
        log_.debug("collection::delete_one : {}", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        auto result = dropped_
                      ? result_delete()
                      : delete_one_(cond);
        send_delete_to_disk_(session, result);
        goblin_engineer::send(dispatcher, address(), "delete_finish", session, result);
    }

    auto collection_t::delete_many(const session_id_t& session, const find_condition_ptr& cond) -> void {
        log_.debug("collection::delete_many : {}", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        auto result = dropped_
                      ? result_delete()
                      : delete_many_(cond);
        send_delete_to_disk_(session, result);
        goblin_engineer::send(dispatcher, address(), "delete_finish", session, result);
    }


    auto collection_t::update_one(const session_id_t& session, const find_condition_ptr& cond, const document_ptr& update, bool upsert) -> void {
        log_.debug("collection::update_one : {}", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        auto result = dropped_
                      ? result_update()
                      : update_one_(cond, update, upsert);
        send_update_to_disk_(session, result);
        goblin_engineer::send(dispatcher, address(), "update_finish", session, result);
    }

    auto collection_t::update_many(const session_id_t& session, const find_condition_ptr& cond, const document_ptr& update, bool upsert) -> void {
        log_.debug("collection::update_many : {}", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        auto result = dropped_
                      ? result_update()
                      : update_many_(cond, update, upsert);
        send_update_to_disk_(session, result);
        goblin_engineer::send(dispatcher, address(), "update_finish", session, result);
    }

    void collection_t::drop(const session_id_t& session) {
        log_.debug("collection::drop : {}", type());
        auto dispatcher = address_book("dispatcher");
        log_.debug("dispatcher : {}", dispatcher.type());
        goblin_engineer::send(dispatcher, address(), "drop_collection_finish_collection", session, result_drop_collection(drop_()), std::string(database_.type()), std::string(type()));
    }

    document_id_t collection_t::insert_(const document_ptr&document) {
        document_view_t view(document);
        auto id = document_id_t(view.get_string("_id"));
        if (storage_.contains(id)) {
            //todo error primary key
        } else {
            storage_.insert_or_assign(id, document);
            return id;
        }
        return document_id_t::null();
    }

    document_view_t collection_t::get_(const document_id_t& id) const {
        if (storage_.contains(id)) {
            const auto &doc = storage_.at(id);
            return document_view_t(doc);
        } else {
            //todo error not valid id
        }
        return document_view_t();
    }

    std::size_t collection_t::size_() const {
        return static_cast<size_t>(storage_.size());
    }

    bool collection_t::drop_() {
        if (dropped_) {
            return false;
        }
        dropped_ = true;
        return true;
    }

    result_find collection_t::search_(const find_condition_ptr& cond) {
        result_find::result_t res;
        for (auto &it : storage_) {
            if (!cond || cond->is_fit(it.second)) {
                res.push_back(document_view_t(it.second));
            }
        }
        return result_find(std::move(res));
    }

    result_find_one collection_t::search_one_(const find_condition_ptr& cond) {
        for (auto &it : storage_) {
            if (!cond || cond->is_fit(it.second)) {
                return result_find_one(document_view_t(it.second));
            }
        }
        return result_find_one();
    }

    result_delete collection_t::delete_one_(const find_condition_ptr& cond) {
        auto finded_doc = search_one_(cond);
        if (finded_doc.is_find()) {
            auto id = document_id_t(finded_doc->get_string("_id"));
            remove_(id);
            return result_delete({id});
        }
        return result_delete();
    }

    result_delete collection_t::delete_many_(const find_condition_ptr& cond) {
        result_delete::result_t deleted;
        auto finded_docs = search_(cond);
        for (auto finded_doc : *finded_docs) {
            auto id = document_id_t(finded_doc.get_string("_id"));
            deleted.push_back(id);
        }
        for (const auto& id : deleted) {
            remove_(id);
        }
        return result_delete(std::move(deleted));
    }

    result_update collection_t::update_one_(const find_condition_ptr& cond, const document_ptr& update, bool upsert) {
        auto finded_doc = search_one_(cond);
        if (finded_doc.is_find()) {
            auto id = document_id_t(finded_doc->get_string("_id"));
            auto res = update_(id, update, true)
                       ? result_update({id}, {})
                       : result_update({}, {id});
            return res;
        }
        if (upsert) {
            return result_update(insert_(components::document::make_upsert_document(update)));
        }
        return result_update();
    }

    result_update collection_t::update_many_(const find_condition_ptr& cond, const document_ptr& update, bool upsert) {
        result_update::result_t modified;
        result_update::result_t nomodified;
        auto finded_docs = search_(cond);
        for (const auto& finded_doc : *finded_docs) {
            auto id = document_id_t(finded_doc.get_string("_id"));
            if (update_(id, update, true)) {
                modified.push_back(id);
            } else {
                nomodified.push_back(id);
            }
        }
        if (upsert && modified.empty() && nomodified.empty()) {
            return result_update(insert_(components::document::make_upsert_document(update)));
        }
        return result_update(std::move(modified), std::move(nomodified));
    }

    void collection_t::remove_(const document_id_t& id) {
        storage_.erase(storage_.find(id));
    }

    bool collection_t::update_(const document_id_t& id, const document_ptr& update, bool is_commit) {
        auto &document = storage_.at(id);
        if (document) {
            if (document->update(*update) && is_commit) {
                document->commit();
                return true;
            }
        }
        return false;
    }

    void collection_t::send_update_to_disk_(const session_id_t& session, const result_update &result) {
        std::vector<document_ptr> update_documents;
        for (const auto &id : result.modified_ids()) {
            update_documents.push_back(storage_.at(id));
        }
        if (!result.upserted_id().is_null()) {
            update_documents.push_back(storage_.at(result.upserted_id()));
        }
        if (!update_documents.empty()) {
            goblin_engineer::send(mdisk_, address(), disk::route::write_documents, session, std::string(database_.type()), std::string(type()), update_documents);
        }
    }

    void collection_t::send_delete_to_disk_(const session_id_t& session, const result_delete &result) {
        if (!result.empty()) {
            goblin_engineer::send(mdisk_, address(), disk::route::remove_documents, session, std::string(database_.type()), std::string(type()), result.deleted_ids());
        }
    }


    void collection_t::close_cursor(session_id_t& session) {
        cursor_storage_.erase(session);
    }

#ifdef DEV_MODE
    void collection_t::insert_test(const document_ptr &doc) {
        insert_(doc);
    }

    result_find collection_t::find_test(find_condition_ptr cond) {
        return search_(cond);
    }

    std::size_t collection_t::size_test() const {
        return size_();
    }

    document_view_t collection_t::get_test(const std::string& id) const {
        return get_(document_id_t(id));
    }

    result_delete collection_t::delete_one_test(find_condition_ptr cond) {
        return delete_one_(cond);
    }

    result_delete collection_t::delete_many_test(find_condition_ptr cond) {
        return delete_many_(cond);
    }

    result_update collection_t::update_one_test(find_condition_ptr cond, const document_ptr& update, bool upsert) {
        return update_one_(cond, update, upsert);
    }

    result_update collection_t::update_many_test(find_condition_ptr cond, const document_ptr& update, bool upsert) {
        return update_many_(cond, update, upsert);
    }

#endif

} // namespace services::storage

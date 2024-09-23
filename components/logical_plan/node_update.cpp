#include "node_update.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"
#include <sstream>

namespace components::logical_plan {

    node_update_t::node_update_t(std::pmr::memory_resource* resource,
                                 const collection_full_name_t& collection,
                                 const components::ql::aggregate::match_t& match,
                                 const components::ql::limit_t& limit,
                                 const components::document::document_ptr& update,
                                 bool upsert)
        : node_t(resource, node_type::update_t, collection)
        , update_(update)
        , upsert_(upsert) {
        append_child(make_node_match(resource, collection, match));
        append_child(make_node_limit(resource, collection, limit));
    }

    const document::document_ptr& node_update_t::update() const { return update_; }

    bool node_update_t::upsert() const { return upsert_; }

    hash_t node_update_t::hash_impl() const { return 0; }

    std::string node_update_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$update: {";
        stream << update_->to_json() << ", ";
        stream << "$upsert: " << upsert_ << ", ";
        bool is_first = true;
        for (auto child : children()) {
            if (!is_first) {
                stream << ", ";
            } else {
                is_first = false;
            }
            stream << child;
        }
        stream << "}";
        return stream.str();
    }

    node_ptr make_node_update(std::pmr::memory_resource* resource, ql::update_many_t* ql) {
        auto node = new node_update_t{resource,
                                      {ql->database_, ql->collection_},
                                      ql->match_,
                                      components::ql::limit_t::unlimit(),
                                      ql->update_,
                                      ql->upsert_};
        return node;
    }

    node_ptr make_node_update(std::pmr::memory_resource* resource, ql::update_one_t* ql) {
        auto node = new node_update_t{resource,
                                      {ql->database_, ql->collection_},
                                      ql->match_,
                                      components::ql::limit_t::limit_one(),
                                      ql->update_,
                                      ql->upsert_};
        return node;
    }

} // namespace components::logical_plan

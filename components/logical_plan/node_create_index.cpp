#include "node_create_index.hpp"
#include <components/types/types.hpp>
#include <sstream>

namespace components::logical_plan {

    node_create_index_t::node_create_index_t(std::pmr::memory_resource* resource,
                                             const collection_full_name_t& collection,
                                             components::ql::create_index_t* ql)
        : node_t(resource, node_type::create_index_t, collection)
        , ql_{ql} {}

    components::ql::create_index_t* node_create_index_t::get_ql() const { return ql_.release(); }

    hash_t node_create_index_t::hash_impl() const { return 0; }

    std::string node_create_index_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_index: " << database_name() << "." << collection_name() << " name:" << ql_->name() << "[ ";
        for (const auto& key : ql_->keys_) {
            stream << key.as_string() << ' ';
        }
        stream << "] type:" << name_index_type(ql_->index_type_) << "compare: " << type_name(ql_->index_compare_);
        return stream.str();
    }

    node_ptr make_node_create_index(std::pmr::memory_resource* resource, components::ql::create_index_t* ql) {
        auto node = new node_create_index_t{resource, {ql->database_, ql->collection_}, ql};
        return node;
    }

} // namespace components::logical_plan

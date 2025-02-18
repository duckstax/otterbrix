#include "planner.hpp"

#include <components/ql/aggregate.hpp>

#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_database.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_update.hpp>

namespace components::planner {
    auto planner_t::translator_aggregate_(std::pmr::memory_resource* resource, ql::aggregate_statement* aggregate)
        -> logical_plan::node_ptr {
        using components::ql::aggregate::operator_type;

        auto node = new logical_plan::node_aggregate_t{resource, {aggregate->database_, aggregate->collection_}};
        auto count = aggregate->count_operators();
        if (aggregate->data) {
            node->reserve_child(count + 1);
            node->append_child(create_plan(resource, aggregate->data.get()));
        } else {
            node->reserve_child(count);
        }
        for (std::size_t i = 0; i < count; ++i) {
            auto type = aggregate->type_operator(i);
            switch (type) {
                case operator_type::match:
                    node->append_child(
                        logical_plan::make_node_match(resource,
                                                      node->collection_full_name(),
                                                      aggregate->get_operator<ql::aggregate::match_t>(i)));
                    break;
                case operator_type::group:
                    node->append_child(
                        logical_plan::make_node_group(resource,
                                                      node->collection_full_name(),
                                                      aggregate->get_operator<ql::aggregate::group_t>(i)));
                    break;
                case operator_type::sort:
                    node->append_child(logical_plan::make_node_sort(resource,
                                                                    node->collection_full_name(),
                                                                    aggregate->get_operator<ql::aggregate::sort_t>(i)));
                    break;
                default:
                    break;
            }
        }
        return node;
    }

    auto planner_t::translator_join_(std::pmr::memory_resource* resource, ql::join_t* ql) -> logical_plan::node_ptr {
        auto node = new logical_plan::node_join_t{resource, {ql->left->database_, ql->left->collection_}, ql->join};
        node->append_child(create_plan(resource, ql->left.get()));
        node->append_child(create_plan(resource, ql->right.get()));
        node->append_expressions(ql->expressions);
        return node;
    }

    auto planner_t::create_plan(std::pmr::memory_resource* resource, ql::ql_statement_t* statement)
        -> logical_plan::node_ptr {
        assert(resource && statement);
        using ql::statement_type;

        switch (statement->type_) {
            case statement_type::unused: {
                throw std::logic_error("unused statement");
                break;
            }
            case statement_type::create_database:
                return logical_plan::make_node_create_database(resource,
                                                               static_cast<ql::create_database_t*>(statement));
            case statement_type::drop_database:
                return logical_plan::make_node_drop_database(resource, static_cast<ql::drop_database_t*>(statement));
            case statement_type::create_collection:
                return logical_plan::make_node_create_collection(resource,
                                                                 static_cast<ql::create_collection_t*>(statement));
            case statement_type::drop_collection:
                return logical_plan::make_node_drop_collection(resource,
                                                               static_cast<ql::drop_collection_t*>(statement));
            case statement_type::create_index:
                return logical_plan::make_node_create_index(resource, static_cast<ql::create_index_t*>(statement));
            case statement_type::drop_index:
                return logical_plan::make_node_drop_index(resource, static_cast<ql::drop_index_t*>(statement));
            case statement_type::insert_one:
                return logical_plan::make_node_insert(resource, static_cast<ql::insert_one_t*>(statement));
            case statement_type::insert_many:
                return logical_plan::make_node_insert(resource, static_cast<ql::insert_many_t*>(statement));
            case statement_type::delete_one:
                return logical_plan::make_node_delete(resource, static_cast<ql::delete_one_t*>(statement));
            case statement_type::delete_many:
                return logical_plan::make_node_delete(resource, static_cast<ql::delete_many_t*>(statement));
            case statement_type::update_one:
                return logical_plan::make_node_update(resource, static_cast<ql::update_one_t*>(statement));
            case statement_type::update_many:
                return logical_plan::make_node_update(resource, static_cast<ql::update_many_t*>(statement));
            case statement_type::raw_data:
                return logical_plan::make_node_data(resource, static_cast<ql::raw_data_t*>(statement));
            case statement_type::aggregate:
                return translator_aggregate_(resource, static_cast<ql::aggregate_statement*>(statement));
            case statement_type::join:
                return translator_join_(resource, static_cast<ql::join_t*>(statement));
            default:
                throw std::logic_error("invalid statement");
        }
    }

} // namespace components::planner

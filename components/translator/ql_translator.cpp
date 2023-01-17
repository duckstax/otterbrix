#include "ql_translator.hpp"

#include <components/ql/aggregate.hpp>

#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_sort.hpp>

namespace components::translator {

    auto translator_aggregate(std::pmr::memory_resource *resource, ql::aggregate_statement* aggregate) -> logical_plan::node_ptr {
        using components::ql::aggregate::operator_type;

        auto node = new logical_plan::node_aggregate_t{resource, {aggregate->database_, aggregate->collection_}};
        auto count = aggregate->count_operators();
        node->reserve_child(count);
        for (std::size_t i = 0; i < count; ++i) {
            auto type = aggregate->type_operator(i);
            switch (type) {
                case operator_type::match:
                    node->append_child(logical_plan::make_node_match(resource, node->collection_full(), aggregate->get_operator<ql::aggregate::match_t>(i)));
                    break;
                case operator_type::group:
                    node->append_child(logical_plan::make_node_group(resource, node->collection_full(), aggregate->get_operator<ql::aggregate::group_t>(i)));
                    break;
                case operator_type::sort:
                    node->append_child(logical_plan::make_node_sort(resource, node->collection_full(), aggregate->get_operator<ql::aggregate::sort_t>(i)));
                    break;
                default:
                    break;
            }
        }
        return node;
    }


    auto ql_translator(std::pmr::memory_resource *resource, ql::ql_statement_t* statement) -> logical_plan::node_ptr {
        using ql::statement_type;

        switch (statement->type_) {
            case statement_type::unused: {
                throw std::logic_error("unused statement");
                break;
            }
            case statement_type::create_database: {
                break;
            }
            case statement_type::drop_database: {
                break;
            }
            case statement_type::create_collection: {
                break;
            }
            case statement_type::drop_collection: {
                break;
            }
            case statement_type::find: {
                break;
            }
            case statement_type::find_one: {
                break;
            }
            case statement_type::insert_one: {
                break;
            }
            case statement_type::insert_many: {
                break;
            }
            case statement_type::delete_one: {
                break;
            }
            case statement_type::delete_many: {
                break;
            }
            case statement_type::update_one: {
                break;
            }
            case statement_type::update_many: {
                break;
            }
            case statement_type::create_index: {
                break;
            }
            case statement_type::aggregate:
                return translator_aggregate(resource, static_cast<ql::aggregate_statement*>(statement));
            default:
                throw std::logic_error("invalid statement");
        }
    }

} // namespace components::ql
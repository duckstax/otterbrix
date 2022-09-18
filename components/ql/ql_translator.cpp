#include "ql_translator.hpp"

#include "components/logical_plan/logical_plan.hpp"


#include "aggregate.hpp"
#include "find.hpp"

namespace components::ql {

    auto translator_aggregate(const aggregate_statement& aggregate) -> logical_plan::node_ptr {

    }

    auto translator_find(const find_statement& aggregate) -> logical_plan::node_ptr {

    }

    auto ql_translator(const ql_statement_t& statement) -> result_translator_ptr {
        auto* result = new result_translator;
        switch (statement.type_) {
            case statement_type::unused: {
                throw std::logic_error("");
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
                translator_find();
                break;
            }
            case statement_type::find_one: {
                translator_find();
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
            case statement_type::aggregate: {
                translator_aggregate(static_cast<aggregate_t*>(statement));
                break;
            }
            default:
                throw std::logic_error("");
        }
        return {result};
    }

} // namespace components::ql
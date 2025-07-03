#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

namespace components::sql::transform {
    // It is guaranteed to be a table ref, but in form of a list of strings
    enum table_name
    {
        table = 1,
        database_table = 2,
        database_schema_table = 3,
        uuid_database_schema_table = 4
    };

    logical_plan::node_ptr transformer::transform_create_table(CreateStmt& node) {
        return logical_plan::make_node_create_collection(resource, rangevar_to_collection(node.relation));
    }

    logical_plan::node_ptr transformer::transform_drop(DropStmt& node) {
        switch (node.removeType) {
            case OBJECT_TABLE: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                switch (static_cast<table_name>(drop_name.size())) {
                    case table: {
                        return logical_plan::make_node_drop_collection(
                            resource,
                            {database_name_t(), strVal(drop_name.front().data)});
                    }
                    case database_table: {
                        auto it = drop_name.begin();
                        auto database = strVal(it++->data);
                        auto collection = strVal(it->data);
                        return logical_plan::make_node_drop_collection(resource, {database, collection});
                    }
                    case database_schema_table: {
                        auto it = drop_name.begin();
                        auto database = strVal(it++->data);
                        auto schema = strVal(it++->data);
                        auto collection = strVal(it->data);
                        return logical_plan::make_node_drop_collection(resource, {database, schema, collection});
                    }
                    case uuid_database_schema_table: {
                        auto it = drop_name.begin();
                        auto uuid = strVal(it++->data);
                        auto database = strVal(it++->data);
                        auto schema = strVal(it++->data);
                        auto collection = strVal(it->data);
                        return logical_plan::make_node_drop_collection(resource, {uuid, database, schema, collection});
                    }
                    default:
                        throw parser_exception_t{"incorrect drop: arguments size", ""};
                        return logical_plan::make_node_drop_collection(resource, {});
                }
            }
            case OBJECT_INDEX: {
                auto drop_name = reinterpret_cast<List*>(node.objects->lst.front().data)->lst;
                if (drop_name.empty()) {
                    throw parser_exception_t{"incorrect drop: arguments size", ""};
                }

                //when casting to enum -1 is used to account for obligated index name
                switch (static_cast<table_name>(drop_name.size() - 1)) {
                    case database_table: {
                        auto it = drop_name.begin();
                        auto database = strVal(it++->data);
                        auto collection = strVal(it++->data);
                        auto name = strVal(it->data);
                        return logical_plan::make_node_drop_index(resource, {database, collection}, name);
                    }
                    case database_schema_table: {
                        auto it = drop_name.begin();
                        auto database = strVal(it++->data);
                        auto schema = strVal(it++->data);
                        auto collection = strVal(it++->data);
                        auto name = strVal(it->data);
                        return logical_plan::make_node_drop_index(resource, {database, schema, collection}, name);
                    }
                    case uuid_database_schema_table: {
                        auto it = drop_name.begin();
                        auto uuid = strVal(it++->data);
                        auto database = strVal(it++->data);
                        auto schema = strVal(it++->data);
                        auto collection = strVal(it++->data);
                        auto name = strVal(it->data);
                        return logical_plan::make_node_drop_index(resource, {uuid, database, schema, collection}, name);
                    }
                    default:
                        throw parser_exception_t{"incorrect drop: arguments size", ""};
                        return logical_plan::make_node_drop_index(resource, {}, "");
                }
            }
            default:
                throw std::runtime_error("Unsupported removeType");
        }
    }

} // namespace components::sql::transform

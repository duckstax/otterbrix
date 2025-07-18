#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::types;

namespace {
    using namespace components::sql::transform;

    complex_logical_type get_type(TypeName* type);

    complex_logical_type get_nested_type(List* nested_types) {
        assert(list_length(nested_types) > 1); // root type + at least 1 nested type

        auto root = pg_ptr_assert_cast<TypeName>(linitial(nested_types), T_TypeName);
        auto nested = pg_ptr_assert_cast<List>(lsecond(nested_types), T_List);
        switch (get_nested_logical_type(strVal(linitial(root->names)))) {
            case logical_type::STRUCT: {
                std::vector<complex_logical_type> columns;
                columns.reserve(list_length(nested));

                size_t cnt = 0;
                for (auto data : nested->lst) {
                    auto type = pg_ptr_assert_cast<TypeName>(data.data, T_TypeName);
                    columns.push_back(get_type(type));
                    columns.back().set_alias(std::to_string(++cnt));
                }

                return complex_logical_type::create_struct(columns);
            }
            case logical_type::LIST: {
                if (list_length(nested) != 1) {
                    throw parser_exception_t{"Incorrect nested types for list type, {node} required", ""};
                }
                auto type = pg_ptr_assert_cast<TypeName>(linitial(nested), T_TypeName);
                auto log_type = get_type(type);

                log_type.set_alias("node");
                return complex_logical_type::create_list(log_type);
            }
            case logical_type::MAP: {
                if (list_length(nested) != 2) {
                    throw parser_exception_t{"Incorrect nested types for map type, {key, value} required", ""};
                }

                auto key = pg_ptr_assert_cast<TypeName>(linitial(nested), T_TypeName);
                auto value = pg_ptr_assert_cast<TypeName>(lsecond(nested), T_TypeName);
                auto log_key = get_type(key);
                auto log_value = get_type(value);

                log_key.set_alias("key");
                log_value.set_alias("value");
                return complex_logical_type::create_map(log_key, log_value);
            }
        }
    }

    complex_logical_type get_type(TypeName* type) {
        if (!list_length(type->names)) {
            // nested case
            assert(!type->arrayBounds);
            return get_nested_type(type->typmods);
        }

        bool is_system = false;
        complex_logical_type column;
        if (auto linint_name = strVal(linitial(type->names)); !std::strcmp(linint_name, "pg_catalog")) {
            is_system = true;
            if (auto col = get_logical_type(strVal(lsecond(type->names))); col != logical_type::DECIMAL) {
                column = col;
            } else {
                if (list_length(type->typmods) != 2) {
                    throw parser_exception_t{"Incorrect modifiers DECIMAL, width and scale required", ""};
                }

                auto width = pg_ptr_assert_cast<A_Const>(linitial(type->typmods), T_A_Const);
                auto scale = pg_ptr_assert_cast<A_Const>(lsecond(type->typmods), T_A_Const);

                if (width->val.type != scale->val.type || width->val.type != T_Integer) {
                    throw parser_exception_t{"Incorrect width or scale for DECIMAL, must be integer", ""};
                }
                column = complex_logical_type::create_decimal(static_cast<uint8_t>(intVal(&width->val)),
                                                              static_cast<uint8_t>(intVal(&scale->val)));
            }
        } else {
            column = get_logical_type(linint_name);
        }

        if (list_length(type->arrayBounds)) {
            if (column.type() == logical_type::NA) {
                throw parser_exception_t(
                    "Array of nested type cannot be created: " +
                        std::string((is_system) ? strVal(lsecond(type->names)) : strVal(linitial(type->names))),
                    "");
            }

            auto size = pg_ptr_assert_cast<Value>(linitial(type->arrayBounds), T_Value);
            if (size->type != T_Integer) {
                throw parser_exception_t{
                    "Incorrect array size of type " +
                        std::string((is_system) ? strVal(lsecond(type->names)) : strVal(linitial(type->names))),
                    ""};
            }
            column = complex_logical_type::create_array(column, intVal(size));
        }

        return column;
    }
} // namespace

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
        auto coldefs = reinterpret_cast<List*>(node.tableElts);

        std::vector<complex_logical_type> columns;
        columns.reserve(list_length(coldefs));
        for (auto data : coldefs->lst) {
            auto coldef = pg_ptr_assert_cast<ColumnDef>(data.data, T_ColumnDef);
            complex_logical_type col = get_type(coldef->typeName);

            if (col.type() == logical_type::NA) {
                throw parser_exception_t{"Unknown type for column: " + std::string(coldef->colname), ""};
            }

            col.set_alias(coldef->colname);
            columns.push_back(std::move(col));
        }

        if (columns.empty()) {
            return logical_plan::make_node_create_collection(resource, rangevar_to_collection(node.relation));
        }

        return logical_plan::make_node_create_collection(resource,
                                                         rangevar_to_collection(node.relation),
                                                         complex_logical_type::create_struct(columns));
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

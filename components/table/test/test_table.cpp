#include <catch2/catch.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>
#include <filesystem>

TEST_CASE("data_table_t") {
    using namespace components::types;
    using namespace components::vector;
    using namespace components::table;

    struct test_struct {
        bool flag;
        int32_t number;
        std::string name;
        std::vector<uint16_t> array;

        test_struct(bool flag, int32_t number, std::string name, std::vector<uint16_t> array)
            : flag(flag)
            , number(number)
            , name(std::move(name))
            , array(std::move(array)) {}
    };

    constexpr size_t test_size = DEFAULT_VECTOR_CAPACITY;
    constexpr size_t array_size = 128;
    constexpr size_t max_list_size = 128;
    auto list_length = [&](size_t i) { return i - (i / max_list_size) * max_list_size; };

    std::vector<complex_logical_type> fields;
    fields.emplace_back(logical_type::BOOLEAN);
    fields.back().set_alias("flag");
    fields.emplace_back(logical_type::INTEGER);
    fields.back().set_alias("number");
    fields.emplace_back(logical_type::STRING_LITERAL);
    fields.back().set_alias("name");
    fields.emplace_back(complex_logical_type::create_list(logical_type::USMALLINT));
    fields.back().set_alias("array");
    complex_logical_type struct_type = complex_logical_type::create_struct(fields);
    struct_type.set_alias("test_struct");

    core::filesystem::local_file_system_t fs;
    auto buffer_pool =
        storage::buffer_pool_t(std::pmr::get_default_resource(), uint64_t(1) << 32, false, uint64_t(1) << 24);
    auto buffer_manager = storage::standard_buffer_manager_t(std::pmr::get_default_resource(), fs, buffer_pool);
    auto block_manager = storage::in_memory_block_manager_t(buffer_manager, storage::DEFAULT_BLOCK_ALLOC_SIZE);

    std::vector<column_definition_t> columns;
    columns.reserve(7);
    columns.emplace_back("temp_column_name0", logical_type::UBIGINT);
    columns.emplace_back("temp_column_name1", logical_type::STRING_LITERAL);
    columns.emplace_back("temp_column_name2", complex_logical_type::create_array(logical_type::UBIGINT, array_size));
    columns.emplace_back("temp_column_name3",
                         complex_logical_type::create_array(logical_type::STRING_LITERAL, array_size));
    columns.emplace_back("temp_column_name4", complex_logical_type::create_list(logical_type::UBIGINT));
    columns.emplace_back("temp_column_name5", complex_logical_type::create_list(logical_type::STRING_LITERAL));
    columns.emplace_back("temp_column_name6", struct_type);

    std::vector<test_struct> test_data;
    test_data.reserve(test_size);
    for (size_t i = 0; i < test_size; i++) {
        auto s{std::string{"long_string_with_index_" + std::to_string(i)}};
        std::vector<uint16_t> arr;
        arr.reserve(i);
        for (size_t j = 0; j < i; j++) {
            arr.emplace_back(j);
        }
        test_data.emplace_back((bool) (i % 2), i, std::move(s), std::move(arr));
    }

    auto data_table =
        std::make_unique<data_table_t>(std::pmr::get_default_resource(), block_manager, std::move(columns));

    INFO("Append") {
        // set up a DataChunks
        data_chunk_t chunk(std::pmr::get_default_resource(), data_table->types(), test_size);
        chunk.set_cardinality(test_size);
        for (size_t i = 0; i < test_size; i++) {
            // UBIGINT
            { chunk.set_value(0, i, logical_value_t{uint64_t(i)}); }
            // STRING
            { chunk.set_value(1, i, logical_value_t{std::string{"long_string_with_index_" + std::to_string(i)}}); }
            // ARRAY<UBIGINT>
            {
                std::vector<logical_value_t> arr;
                arr.reserve(array_size);
                for (size_t j = 0; j < array_size; j++) {
                    arr.emplace_back(uint64_t{i * array_size + j});
                }
                chunk.set_value(2, i, logical_value_t::create_array(logical_type::UBIGINT, arr));
            }
            // ARRAY<STRING>
            {
                std::vector<logical_value_t> arr;
                arr.reserve(array_size);
                for (size_t j = 0; j < array_size; j++) {
                    arr.emplace_back(std::string{"long_string_with_index_" + std::to_string(i * array_size + j)});
                }
                chunk.set_value(3, i, logical_value_t::create_array(logical_type::STRING_LITERAL, arr));
            }
            // LIST<UBIGINT>
            {
                std::vector<logical_value_t> list;
                // test that each list entry can be a different length
                list.reserve(list_length(i));
                for (size_t j = 0; j < list_length(i); j++) {
                    list.emplace_back(uint64_t{i * list_length(i) + j});
                }
                chunk.set_value(4, i, logical_value_t::create_list(logical_type::UBIGINT, list));
            }
            // LIST<STRING>
            {
                std::vector<logical_value_t> list;
                // test that each list entry can be a different length
                list.reserve(list_length(i));
                for (size_t j = 0; j < list_length(i); j++) {
                    list.emplace_back(std::string{"long_string_with_index_" + std::to_string(i * list_length(i) + j)});
                }
                chunk.set_value(5, i, logical_value_t::create_list(logical_type::STRING_LITERAL, list));
            }
            // STRUCT
            {
                std::vector<logical_value_t> arr;
                arr.reserve(i);
                for (size_t j = 0; j < i; j++) {
                    arr.emplace_back(test_data[i].array[j]);
                }
                std::vector<logical_value_t> value_fiels;
                value_fiels.emplace_back(logical_value_t{test_data[i].flag});
                value_fiels.emplace_back(logical_value_t{test_data[i].number});
                value_fiels.emplace_back(logical_value_t{test_data[i].name});
                value_fiels.emplace_back(logical_value_t::create_list(logical_type::USMALLINT, arr));
                logical_value_t value = logical_value_t::create_struct(struct_type, value_fiels);
                chunk.set_value(6, i, value);
            }
        }

        table_append_state state(std::pmr::get_default_resource());
        data_table->append_lock(state);
        data_table->initialize_append(state);
        data_table->append(chunk, state);
        data_table->finalize_append(state);
    }
    INFO("Fetch") {
        column_fetch_state state;
        std::vector<storage_index_t> column_indices;
        column_indices.reserve(7);
        for (int64_t i = 0; i < 7; i++) {
            column_indices.emplace_back(i);
        }
        vector_t rows(std::pmr::get_default_resource(), logical_type::BIGINT);
        for (int64_t i = 0; i < test_size; i++) {
            rows.set_value(i, logical_value_t(i));
        }
        data_chunk_t result(std::pmr::get_default_resource(), data_table->types());
        data_table->fetch(result, column_indices, rows, test_size, state);

        for (size_t i = 0; i < test_size; i++) {
            // UBIGINT
            {
                logical_value_t value = result.data[0].value(i);
                REQUIRE(value.type().type() == logical_type::UBIGINT);
                REQUIRE(value.value<uint64_t>() == i);
            }
            // STRING
            {
                logical_value_t value = result.data[1].value(i);
                REQUIRE(value.type().type() == logical_type::STRING_LITERAL);
                std::string result = *(value.value<std::string*>());
                REQUIRE(result == std::string{"long_string_with_index_" + std::to_string(i)});
            }
            // ARRAY<UBIGINT>
            {
                logical_value_t value = result.data[2].value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == i * array_size + j);
                }
            }
            // ARRAY<STRING>
            {
                logical_value_t value = result.data[3].value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string result = *(value.children()[j].value<std::string*>());
                    REQUIRE(result == std::string{"long_string_with_index_" + std::to_string(i * array_size + j)});
                }
            }
            // LIST<UBIGINT>
            {
                logical_value_t value = result.data[4].value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == i * list_length(i) + j);
                }
            }
            // LIST<STRING>
            {
                logical_value_t value = result.data[5].value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string result = *(value.children()[j].value<std::string*>());
                    REQUIRE(result == std::string{"long_string_with_index_" + std::to_string(i * list_length(i) + j)});
                }
            }
            // STRUCT
            {
                logical_value_t value = result.data[6].value(i);
                REQUIRE(value.type().type() == logical_type::STRUCT);
                REQUIRE(value.type().alias() == "test_struct");
                REQUIRE(value.type().child_types()[0].type() == logical_type::BOOLEAN);
                REQUIRE(value.type().child_types()[0].alias() == "flag");
                REQUIRE(value.type().child_types()[1].type() == logical_type::INTEGER);
                REQUIRE(value.type().child_types()[1].alias() == "number");
                REQUIRE(value.type().child_types()[2].type() == logical_type::STRING_LITERAL);
                REQUIRE(value.type().child_types()[2].alias() == "name");
                REQUIRE(value.type().child_types()[3].type() == logical_type::LIST);
                REQUIRE(value.type().child_types()[3].child_type().type() == logical_type::USMALLINT);
                REQUIRE(value.type().child_types()[3].alias() == "array");

                REQUIRE(value.children()[0].value<bool>() == test_data[i].flag);
                REQUIRE(value.children()[1].value<int32_t>() == test_data[i].number);
                REQUIRE(*value.children()[2].value<std::string*>() == test_data[i].name);
                std::vector arr(*value.children()[3].value<std::vector<logical_value_t>*>());
                REQUIRE(arr.size() == test_data[i].array.size());
                for (size_t j = 0; j < arr.size(); j++) {
                    arr[j].value<uint16_t>() == test_data[i].array[j];
                }
            }
        }
    }
    INFO("Scan") {
        std::vector<storage_index_t> column_indices;
        column_indices.reserve(data_table->column_count());
        for (int64_t i = 0; i < data_table->column_count(); i++) {
            column_indices.emplace_back(i);
        }
        table_scan_state state(std::pmr::get_default_resource());
        data_chunk_t result(std::pmr::get_default_resource(), data_table->types());
        data_table->initialize_scan(state, column_indices);
        data_table->scan(result, state);

        for (size_t i = 0; i < test_size; i++) {
            // UBIGINT
            {
                logical_value_t value = result.data[0].value(i);
                REQUIRE(value.type().type() == logical_type::UBIGINT);
                REQUIRE(value.value<uint64_t>() == i);
            }
            // STRING
            {
                logical_value_t value = result.data[1].value(i);
                REQUIRE(value.type().type() == logical_type::STRING_LITERAL);
                std::string result = *(value.value<std::string*>());
                REQUIRE(result == std::string{"long_string_with_index_" + std::to_string(i)});
            }
            // ARRAY<UBIGINT>
            {
                logical_value_t value = result.data[2].value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == i * array_size + j);
                }
            }
            // ARRAY<STRING>
            {
                logical_value_t value = result.data[3].value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string result = *(value.children()[j].value<std::string*>());
                    REQUIRE(result == std::string{"long_string_with_index_" + std::to_string(i * array_size + j)});
                }
            }
            // LIST<UBIGINT>
            {
                logical_value_t value = result.data[4].value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == i * list_length(i) + j);
                }
            }
            // LIST<STRING>
            {
                logical_value_t value = result.data[5].value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string result = *(value.children()[j].value<std::string*>());
                    REQUIRE(result == std::string{"long_string_with_index_" + std::to_string(i * list_length(i) + j)});
                }
            }
            // STRUCT
            {
                logical_value_t value = result.data[6].value(i);
                REQUIRE(value.type().type() == logical_type::STRUCT);
                REQUIRE(value.type().alias() == "test_struct");
                REQUIRE(value.type().child_types()[0].type() == logical_type::BOOLEAN);
                REQUIRE(value.type().child_types()[0].alias() == "flag");
                REQUIRE(value.type().child_types()[1].type() == logical_type::INTEGER);
                REQUIRE(value.type().child_types()[1].alias() == "number");
                REQUIRE(value.type().child_types()[2].type() == logical_type::STRING_LITERAL);
                REQUIRE(value.type().child_types()[2].alias() == "name");
                REQUIRE(value.type().child_types()[3].type() == logical_type::LIST);
                REQUIRE(value.type().child_types()[3].child_type().type() == logical_type::USMALLINT);
                REQUIRE(value.type().child_types()[3].alias() == "array");

                REQUIRE(value.children()[0].value<bool>() == test_data[i].flag);
                REQUIRE(value.children()[1].value<int32_t>() == test_data[i].number);
                REQUIRE(*value.children()[2].value<std::string*>() == test_data[i].name);
                std::vector arr(*value.children()[3].value<std::vector<logical_value_t>*>());
                REQUIRE(arr.size() == test_data[i].array.size());
                for (size_t j = 0; j < arr.size(); j++) {
                    arr[j].value<uint16_t>() == test_data[i].array[j];
                }
            }
        }
    }
    INFO("Delete") {
        vector_t v(std::pmr::get_default_resource(), logical_type::BIGINT, test_size / 2);
        for (size_t i = 0; i < test_size; i += 2) {
            v.set_value(i / 2, logical_value_t(int64_t(i)));
        }
        auto state = data_table->initialize_delete({});
        auto deleted_count = data_table->delete_rows(*state, v, test_size / 2);
        REQUIRE(deleted_count == test_size / 2);
    }
    INFO("Scan after delete") {
        std::vector<storage_index_t> column_indices;
        column_indices.reserve(data_table->column_count());
        for (int64_t i = 0; i < data_table->column_count(); i++) {
            column_indices.emplace_back(i);
        }
        table_scan_state state(std::pmr::get_default_resource());
        data_chunk_t result(std::pmr::get_default_resource(), data_table->types());
        data_table->initialize_scan(state, column_indices);
        data_table->scan(result, state);

        for (size_t i = 0; i < test_size / 2; i++) {
            auto test_data_index = i * 2 + 1;
            // UBIGINT
            {
                logical_value_t value = result.data[0].value(i);
                REQUIRE(value.type().type() == logical_type::UBIGINT);
                REQUIRE(value.value<uint64_t>() == test_data_index);
            }
            // STRING
            {
                logical_value_t value = result.data[1].value(i);
                REQUIRE(value.type().type() == logical_type::STRING_LITERAL);
                std::string result = *(value.value<std::string*>());
                REQUIRE(result == std::string{"long_string_with_index_" + std::to_string(test_data_index)});
            }
            // ARRAY<UBIGINT>
            {
                logical_value_t value = result.data[2].value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == test_data_index * array_size + j);
                }
            }
            // ARRAY<STRING>
            {
                logical_value_t value = result.data[3].value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string result = *(value.children()[j].value<std::string*>());
                    REQUIRE(result ==
                            std::string{"long_string_with_index_" + std::to_string(test_data_index * array_size + j)});
                }
            }
            // LIST<UBIGINT>
            {
                logical_value_t value = result.data[4].value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(test_data_index); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() ==
                            test_data_index * list_length(test_data_index) + j);
                }
            }
            // LIST<STRING>
            {
                logical_value_t value = result.data[5].value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(test_data_index); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string result = *(value.children()[j].value<std::string*>());
                    REQUIRE(result == std::string{"long_string_with_index_" +
                                                  std::to_string(test_data_index * list_length(test_data_index) + j)});
                }
            }
            // STRUCT
            {
                logical_value_t value = result.data[6].value(i);
                REQUIRE(value.type().type() == logical_type::STRUCT);
                REQUIRE(value.type().alias() == "test_struct");
                REQUIRE(value.type().child_types()[0].type() == logical_type::BOOLEAN);
                REQUIRE(value.type().child_types()[0].alias() == "flag");
                REQUIRE(value.type().child_types()[1].type() == logical_type::INTEGER);
                REQUIRE(value.type().child_types()[1].alias() == "number");
                REQUIRE(value.type().child_types()[2].type() == logical_type::STRING_LITERAL);
                REQUIRE(value.type().child_types()[2].alias() == "name");
                REQUIRE(value.type().child_types()[3].type() == logical_type::LIST);
                REQUIRE(value.type().child_types()[3].child_type().type() == logical_type::USMALLINT);
                REQUIRE(value.type().child_types()[3].alias() == "array");

                REQUIRE(value.children()[0].value<bool>() == test_data[test_data_index].flag);
                REQUIRE(value.children()[1].value<int32_t>() == test_data[test_data_index].number);
                REQUIRE(*value.children()[2].value<std::string*>() == test_data[test_data_index].name);
                std::vector arr(*value.children()[3].value<std::vector<logical_value_t>*>());
                REQUIRE(arr.size() == test_data[test_data_index].array.size());
                for (size_t j = 0; j < arr.size(); j++) {
                    arr[j].value<uint16_t>() == test_data[test_data_index].array[j];
                }
            }
        }
    }

    INFO("Extention") {
        std::unique_ptr<data_table_t> extended_table;

        {
            column_definition_t new_column{"temp_column_name7",
                                           logical_type::SMALLINT,
                                           std::make_unique<logical_value_t>(int16_t(0))};
            extended_table = std::make_unique<data_table_t>(*data_table, new_column);

            // Update values in new column
            // Since row mask is applied to every column at once, in update we have to fill whole column
            // or manually calculate ids where set is needed
            vector_t v(std::pmr::get_default_resource(), logical_type::BIGINT, test_size / 2);
            data_chunk_t chunk(std::pmr::get_default_resource(), {logical_type::SMALLINT}, test_size / 2);
            chunk.set_cardinality(test_size / 2);
            for (size_t i = 0; i < test_size / 2; i++) {
                v.set_value(i, logical_value_t(int64_t(i * 2 + 1)));
                chunk.set_value(0, i, logical_value_t{int16_t(i * 2 + 1)});
            }
            extended_table->update_column(v, {7}, chunk);
        }
        // Scan after extention
        {
            std::vector<storage_index_t> column_indices;
            column_indices.reserve(extended_table->column_count());
            for (int64_t i = 0; i < extended_table->column_count(); i++) {
                column_indices.emplace_back(i);
            }
            table_scan_state state(std::pmr::get_default_resource());
            data_chunk_t result(std::pmr::get_default_resource(), extended_table->types());
            extended_table->initialize_scan(state, column_indices);
            extended_table->scan(result, state);

            for (size_t i = 0; i < test_size / 2; i++) {
                auto test_data_index = i * 2 + 1;
                // UBIGINT
                {
                    logical_value_t value = result.data[0].value(i);
                    REQUIRE(value.type().type() == logical_type::UBIGINT);
                    REQUIRE(value.value<uint64_t>() == test_data_index);
                }
                // STRING
                {
                    logical_value_t value = result.data[1].value(i);
                    REQUIRE(value.type().type() == logical_type::STRING_LITERAL);
                    std::string result = *(value.value<std::string*>());
                    REQUIRE(result == std::string{"long_string_with_index_" + std::to_string(test_data_index)});
                }
                // ARRAY<UBIGINT>
                {
                    logical_value_t value = result.data[2].value(i);
                    REQUIRE(value.type().type() == logical_type::ARRAY);
                    for (size_t j = 0; j < array_size; j++) {
                        REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                        REQUIRE(value.children()[j].value<uint64_t>() == test_data_index * array_size + j);
                    }
                }
                // ARRAY<STRING>
                {
                    logical_value_t value = result.data[3].value(i);
                    REQUIRE(value.type().type() == logical_type::ARRAY);
                    for (size_t j = 0; j < array_size; j++) {
                        REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                        std::string result = *(value.children()[j].value<std::string*>());
                        REQUIRE(result == std::string{"long_string_with_index_" +
                                                      std::to_string(test_data_index * array_size + j)});
                    }
                }
                // LIST<UBIGINT>
                {
                    logical_value_t value = result.data[4].value(i);
                    REQUIRE(value.type().type() == logical_type::LIST);
                    for (size_t j = 0; j < list_length(test_data_index); j++) {
                        REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                        REQUIRE(value.children()[j].value<uint64_t>() ==
                                test_data_index * list_length(test_data_index) + j);
                    }
                }
                // LIST<STRING>
                {
                    logical_value_t value = result.data[5].value(i);
                    REQUIRE(value.type().type() == logical_type::LIST);
                    for (size_t j = 0; j < list_length(test_data_index); j++) {
                        REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                        std::string result = *(value.children()[j].value<std::string*>());
                        REQUIRE(result ==
                                std::string{"long_string_with_index_" +
                                            std::to_string(test_data_index * list_length(test_data_index) + j)});
                    }
                }
                // STRUCT
                {
                    logical_value_t value = result.data[6].value(i);
                    REQUIRE(value.type().type() == logical_type::STRUCT);
                    REQUIRE(value.type().alias() == "test_struct");
                    REQUIRE(value.type().child_types()[0].type() == logical_type::BOOLEAN);
                    REQUIRE(value.type().child_types()[0].alias() == "flag");
                    REQUIRE(value.type().child_types()[1].type() == logical_type::INTEGER);
                    REQUIRE(value.type().child_types()[1].alias() == "number");
                    REQUIRE(value.type().child_types()[2].type() == logical_type::STRING_LITERAL);
                    REQUIRE(value.type().child_types()[2].alias() == "name");
                    REQUIRE(value.type().child_types()[3].type() == logical_type::LIST);
                    REQUIRE(value.type().child_types()[3].child_type().type() == logical_type::USMALLINT);
                    REQUIRE(value.type().child_types()[3].alias() == "array");

                    REQUIRE(value.children()[0].value<bool>() == test_data[test_data_index].flag);
                    REQUIRE(value.children()[1].value<int32_t>() == test_data[test_data_index].number);
                    REQUIRE(*value.children()[2].value<std::string*>() == test_data[test_data_index].name);
                    std::vector arr(*value.children()[3].value<std::vector<logical_value_t>*>());
                    REQUIRE(arr.size() == test_data[test_data_index].array.size());
                    for (size_t j = 0; j < arr.size(); j++) {
                        arr[j].value<uint16_t>() == test_data[test_data_index].array[j];
                    }
                }
                // SMALLINT
                {
                    logical_value_t value = result.data[7].value(i);
                    REQUIRE(value.type().type() == logical_type::SMALLINT);
                    REQUIRE(value.value<int16_t>() == test_data_index);
                }
            }
        }

        // Remove column
        std::unique_ptr<data_table_t> short_table = std::make_unique<data_table_t>(*extended_table, 0);

        // Scan after column removal
        {
            std::vector<storage_index_t> column_indices;
            column_indices.reserve(short_table->column_count());
            for (int64_t i = 0; i < short_table->column_count(); i++) {
                column_indices.emplace_back(i);
            }
            table_scan_state state(std::pmr::get_default_resource());
            data_chunk_t result(std::pmr::get_default_resource(), short_table->types());
            short_table->initialize_scan(state, column_indices);
            short_table->scan(result, state);

            for (size_t i = 0; i < test_size / 2; i++) {
                auto test_data_index = i * 2 + 1;
                // STRING
                {
                    logical_value_t value = result.data[0].value(i);
                    REQUIRE(value.type().type() == logical_type::STRING_LITERAL);
                    std::string result = *(value.value<std::string*>());
                    REQUIRE(result == std::string{"long_string_with_index_" + std::to_string(test_data_index)});
                }
                // ARRAY<UBIGINT>
                {
                    logical_value_t value = result.data[1].value(i);
                    REQUIRE(value.type().type() == logical_type::ARRAY);
                    for (size_t j = 0; j < array_size; j++) {
                        REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                        REQUIRE(value.children()[j].value<uint64_t>() == test_data_index * array_size + j);
                    }
                }
                // ARRAY<STRING>
                {
                    logical_value_t value = result.data[2].value(i);
                    REQUIRE(value.type().type() == logical_type::ARRAY);
                    for (size_t j = 0; j < array_size; j++) {
                        REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                        std::string result = *(value.children()[j].value<std::string*>());
                        REQUIRE(result == std::string{"long_string_with_index_" +
                                                      std::to_string(test_data_index * array_size + j)});
                    }
                }
                // LIST<UBIGINT>
                {
                    logical_value_t value = result.data[3].value(i);
                    REQUIRE(value.type().type() == logical_type::LIST);
                    for (size_t j = 0; j < list_length(test_data_index); j++) {
                        REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                        REQUIRE(value.children()[j].value<uint64_t>() ==
                                test_data_index * list_length(test_data_index) + j);
                    }
                }
                // LIST<STRING>
                {
                    logical_value_t value = result.data[4].value(i);
                    REQUIRE(value.type().type() == logical_type::LIST);
                    for (size_t j = 0; j < list_length(test_data_index); j++) {
                        REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                        std::string result = *(value.children()[j].value<std::string*>());
                        REQUIRE(result ==
                                std::string{"long_string_with_index_" +
                                            std::to_string(test_data_index * list_length(test_data_index) + j)});
                    }
                }
                // STRUCT
                {
                    logical_value_t value = result.data[5].value(i);
                    REQUIRE(value.type().type() == logical_type::STRUCT);
                    REQUIRE(value.type().alias() == "test_struct");
                    REQUIRE(value.type().child_types()[0].type() == logical_type::BOOLEAN);
                    REQUIRE(value.type().child_types()[0].alias() == "flag");
                    REQUIRE(value.type().child_types()[1].type() == logical_type::INTEGER);
                    REQUIRE(value.type().child_types()[1].alias() == "number");
                    REQUIRE(value.type().child_types()[2].type() == logical_type::STRING_LITERAL);
                    REQUIRE(value.type().child_types()[2].alias() == "name");
                    REQUIRE(value.type().child_types()[3].type() == logical_type::LIST);
                    REQUIRE(value.type().child_types()[3].child_type().type() == logical_type::USMALLINT);
                    REQUIRE(value.type().child_types()[3].alias() == "array");

                    REQUIRE(value.children()[0].value<bool>() == test_data[test_data_index].flag);
                    REQUIRE(value.children()[1].value<int32_t>() == test_data[test_data_index].number);
                    REQUIRE(*value.children()[2].value<std::string*>() == test_data[test_data_index].name);
                    std::vector arr(*value.children()[3].value<std::vector<logical_value_t>*>());
                    REQUIRE(arr.size() == test_data[test_data_index].array.size());
                    for (size_t j = 0; j < arr.size(); j++) {
                        arr[j].value<uint16_t>() == test_data[test_data_index].array[j];
                    }
                }
                // SMALLINT
                {
                    logical_value_t value = result.data[6].value(i);
                    REQUIRE(value.type().type() == logical_type::SMALLINT);
                    REQUIRE(value.value<int16_t>() == test_data_index);
                }
            }
        }
    }
}
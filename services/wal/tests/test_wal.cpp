#include <catch2/catch.hpp>

#include <actor-zeta.hpp>
#include <crc32c/crc32c.h>
#include <log/log.hpp>
#include <string>

#include <components/document/document.hpp>
#include <components/ql/statements.hpp>
#include <components/tests/generaty.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal.hpp>

using namespace services::wal;
using namespace components::ql;
using namespace components::expressions;

constexpr auto database_name = "test_database";
constexpr auto collection_name = "test_collection";
constexpr std::size_t count_documents = 5;

void test_insert_one(wal_replicate_t* wal) {
    for (int num = 1; num <= 5; ++num) {
        auto document = gen_doc(num, wal->resource());
        insert_one_t data(database_name, collection_name, std::move(document));
        auto session = components::session::session_id_t();
        auto address = actor_zeta::base::address_t::address_t::empty_address();
        wal->insert_one(session, address, data);
    }
}

struct test_wal {
    core::non_thread_scheduler::scheduler_test_t* scheduler{nullptr};
    wal_replicate_t* wal{nullptr};
};

test_wal create_test_wal(const std::filesystem::path& path, std::pmr::memory_resource* resource) {
    test_wal result;
    static auto log = initialization_logger("python", "/tmp/docker_logs/");
    log.set_level(log_t::level::trace);
    result.scheduler = new core::non_thread_scheduler::scheduler_test_t(1, 1);
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    configuration::config_wal config;
    config.path = path;
    auto manager = actor_zeta::spawn_supervisor<manager_wal_replicate_t>(resource, result.scheduler, config, log);
    auto allocate_byte = sizeof(wal_replicate_t);
    auto allocate_byte_alignof = alignof(wal_replicate_t);
    void* buffer = manager->resource()->allocate(allocate_byte, allocate_byte_alignof);
    result.wal = new (buffer) wal_replicate_t(manager.get(), log, config);
    return result;
}

TEST_CASE("insert one test") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/insert_one", &resource);
    test_insert_one(test_wal.wal);

    std::size_t read_index = 0;
    for (int num = 1; num <= 5; ++num) {
        wal_entry_t<insert_one_t> entry(&resource);

        entry.size_ = test_wal.wal->test_read_size(read_index);

        auto start = read_index + sizeof(size_tt);
        auto finish = read_index + sizeof(size_tt) + entry.size_ + sizeof(crc32_t);
        auto output = test_wal.wal->test_read(start, finish);

        auto crc32_index = entry.size_;
        crc32_t crc32 = crc32c::Crc32c(output.data(), crc32_index);

        unpack(output, entry, &resource);
        entry.crc32_ = read_crc32(output, entry.size_);
        test_wal.scheduler->run();
        REQUIRE(entry.crc32_ == crc32);
        REQUIRE(entry.entry_.database_ == database_name);
        REQUIRE(entry.entry_.collection_ == collection_name);
        auto doc = entry.entry_.document_;
        REQUIRE(doc->get_string("/_id") == gen_id(num, &resource));
        REQUIRE(doc->get_long("/count") == num);
        REQUIRE(doc->get_string("/countStr") == std::pmr::string(std::to_string(num), &resource));

        read_index = finish;
    }
}

TEST_CASE("insert many empty test") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/insert_many_empty", &resource);

    std::pmr::vector<components::document::document_ptr> documents(&resource);
    insert_many_t data(database_name, collection_name, std::move(documents));

    auto session = components::session::session_id_t();
    auto address = actor_zeta::base::address_t::address_t::empty_address();
    test_wal.wal->insert_many(session, address, data);

    wal_entry_t<insert_many_t> entry(&resource);

    entry.size_ = test_wal.wal->test_read_size(0);

    auto start = sizeof(size_tt);
    auto finish = sizeof(size_tt) + entry.size_ + sizeof(crc32_t);
    auto output = test_wal.wal->test_read(start, finish);

    auto crc32_index = entry.size_;
    crc32_t crc32 = crc32c::Crc32c(output.data(), crc32_index);

    unpack(output, entry, &resource);
    entry.crc32_ = read_crc32(output, entry.size_);
    test_wal.scheduler->run();
    REQUIRE(entry.crc32_ == crc32);
}

TEST_CASE("insert many test") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/insert_many", &resource);

    for (int i = 0; i <= 3; ++i) {
        std::pmr::vector<components::document::document_ptr> documents(&resource);
        for (int num = 1; num <= 5; ++num) {
            documents.push_back(gen_doc(num, &resource));
        }
        insert_many_t data(database_name, collection_name, std::move(documents));
        auto session = components::session::session_id_t();
        auto address = actor_zeta::base::address_t::address_t::empty_address();
        test_wal.wal->insert_many(session, address, data);
    }

    std::size_t read_index = 0;
    for (int i = 0; i <= 3; ++i) {
        wal_entry_t<insert_many_t> entry(&resource);

        entry.size_ = test_wal.wal->test_read_size(read_index);

        auto start = read_index + sizeof(size_tt);
        auto finish = read_index + sizeof(size_tt) + entry.size_ + sizeof(crc32_t);
        auto output = test_wal.wal->test_read(start, finish);

        auto crc32_index = entry.size_;
        crc32_t crc32 = crc32c::Crc32c(output.data(), crc32_index);

        unpack(output, entry, &resource);
        entry.crc32_ = read_crc32(output, entry.size_);
        test_wal.scheduler->run();
        REQUIRE(entry.crc32_ == crc32);
        REQUIRE(entry.entry_.database_ == database_name);
        REQUIRE(entry.entry_.collection_ == collection_name);
        REQUIRE(entry.entry_.documents_.size() == 5);
        int num = 0;
        for (const auto& doc : entry.entry_.documents_) {
            ++num;
            REQUIRE(doc->get_string("/_id") == gen_id(num, &resource));
            REQUIRE(doc->get_long("/count") == num);
            REQUIRE(doc->get_string("/countStr") == std::pmr::string(std::to_string(num), &resource));
        }

        read_index = finish;
    }
}

TEST_CASE("delete one test") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/delete_one", &resource);

    for (int num = 1; num <= 5; ++num) {
        auto match = aggregate::make_match(make_compare_expression(&resource,
                                                                   compare_type::eq,
                                                                   components::expressions::key_t{"count"},
                                                                   core::parameter_id_t{1}));
        storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t{1}, num);
        delete_one_t data(database_name, collection_name, match, parameters);
        auto session = components::session::session_id_t();
        auto address = actor_zeta::base::address_t::address_t::empty_address();
        test_wal.wal->delete_one(session, address, data);
    }

    std::size_t index = 0;
    for (int num = 1; num <= 5; ++num) {
        auto record = test_wal.wal->test_read_record(index);
        REQUIRE(record.type == statement_type::delete_one);
        REQUIRE(record.id == services::wal::id_t(num));
        REQUIRE(std::get<delete_one_t>(record.data).database_ == database_name);
        REQUIRE(std::get<delete_one_t>(record.data).collection_ == collection_name);
        REQUIRE(std::get<delete_one_t>(record.data).match_.query->group() == expression_group::compare);
        auto match = reinterpret_cast<const compare_expression_ptr&>(std::get<delete_one_t>(record.data).match_.query);
        REQUIRE(match->type() == compare_type::eq);
        REQUIRE(match->key() == components::expressions::key_t{"count"});
        REQUIRE(match->value() == core::parameter_id_t{1});
        REQUIRE(std::get<delete_one_t>(record.data).parameters().parameters.size() == 1);
        REQUIRE(get_parameter(&std::get<delete_one_t>(record.data).parameters(), core::parameter_id_t{1}).as_int() ==
                num);
        index = test_wal.wal->test_next_record(index);
    }
}

TEST_CASE("delete many test") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/delete_many", &resource);

    for (int num = 1; num <= 5; ++num) {
        auto match = aggregate::make_match(make_compare_expression(&resource,
                                                                   compare_type::eq,
                                                                   components::expressions::key_t{"count"},
                                                                   core::parameter_id_t{1}));
        storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t{1}, num);
        delete_many_t data(database_name, collection_name, match, parameters);
        auto session = components::session::session_id_t();
        auto address = actor_zeta::base::address_t::address_t::empty_address();
        test_wal.wal->delete_many(session, address, data);
    }

    std::size_t index = 0;
    for (int num = 1; num <= 5; ++num) {
        auto record = test_wal.wal->test_read_record(index);
        REQUIRE(record.type == statement_type::delete_many);
        REQUIRE(record.id == services::wal::id_t(num));
        REQUIRE(std::get<delete_many_t>(record.data).database_ == database_name);
        REQUIRE(std::get<delete_many_t>(record.data).collection_ == collection_name);
        REQUIRE(std::get<delete_many_t>(record.data).match_.query->group() == expression_group::compare);
        auto match = reinterpret_cast<const compare_expression_ptr&>(std::get<delete_many_t>(record.data).match_.query);
        REQUIRE(match->type() == compare_type::eq);
        REQUIRE(match->key() == components::expressions::key_t{"count"});
        REQUIRE(match->value() == core::parameter_id_t{1});
        REQUIRE(std::get<delete_many_t>(record.data).parameters().parameters.size() == 1);
        REQUIRE(get_parameter(&std::get<delete_many_t>(record.data).parameters(), core::parameter_id_t{1}).as_int() ==
                num);
        index = test_wal.wal->test_next_record(index);
    }
}

TEST_CASE("update one test") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/update_one", &resource);

    for (int num = 1; num <= 5; ++num) {
        auto match = aggregate::make_match(make_compare_expression(&resource,
                                                                   compare_type::eq,
                                                                   components::expressions::key_t{"count"},
                                                                   core::parameter_id_t{1}));
        storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t{1}, num);
        auto update = components::document::document_t::document_from_json(R"({"$set": {"count": )" +
                                                                               std::to_string(num + 10) + "}}",
                                                                           &resource);
        update_one_t data(database_name, collection_name, match, parameters, update, num % 2 == 0);
        auto session = components::session::session_id_t();
        auto address = actor_zeta::base::address_t::address_t::empty_address();
        test_wal.wal->update_one(session, address, data);
    }

    std::size_t index = 0;
    for (int num = 1; num <= 5; ++num) {
        auto record = test_wal.wal->test_read_record(index);
        REQUIRE(record.type == statement_type::update_one);
        REQUIRE(record.id == services::wal::id_t(num));
        REQUIRE(std::get<update_one_t>(record.data).database_ == database_name);
        REQUIRE(std::get<update_one_t>(record.data).collection_ == collection_name);
        REQUIRE(std::get<update_one_t>(record.data).match_.query->group() == expression_group::compare);
        auto match = reinterpret_cast<const compare_expression_ptr&>(std::get<update_one_t>(record.data).match_.query);
        REQUIRE(match->type() == compare_type::eq);
        REQUIRE(match->key() == components::expressions::key_t{"count"});
        REQUIRE(match->value() == core::parameter_id_t{1});
        REQUIRE(std::get<update_one_t>(record.data).parameters().parameters.size() == 1);
        REQUIRE(get_parameter(&std::get<update_one_t>(record.data).parameters(), core::parameter_id_t{1}).as_int() ==
                num);
        auto doc = std::get<update_one_t>(record.data).update_;
        REQUIRE(doc->get_dict("$set")->get_long("count") == num + 10);
        REQUIRE(std::get<update_one_t>(record.data).upsert_ == (num % 2 == 0));
        index = test_wal.wal->test_next_record(index);
    }
}

TEST_CASE("update many test") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/update_many", &resource);

    for (int num = 1; num <= 5; ++num) {
        auto match = aggregate::make_match(make_compare_expression(&resource,
                                                                   compare_type::eq,
                                                                   components::expressions::key_t{"count"},
                                                                   core::parameter_id_t{1}));
        storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t{1}, num);
        auto update =
            document_t::document_from_json(R"({"$set": {"count": )" + std::to_string(num + 10) + "}}", &resource);
        update_many_t data(database_name, collection_name, match, parameters, update, num % 2 == 0);
        auto session = components::session::session_id_t();
        auto address = actor_zeta::base::address_t::address_t::empty_address();
        test_wal.wal->update_many(session, address, data);
    }

    std::size_t index = 0;
    for (int num = 1; num <= 5; ++num) {
        auto record = test_wal.wal->test_read_record(index);
        REQUIRE(record.type == statement_type::update_many);
        REQUIRE(record.id == services::wal::id_t(num));
        REQUIRE(std::get<update_many_t>(record.data).database_ == database_name);
        REQUIRE(std::get<update_many_t>(record.data).collection_ == collection_name);
        REQUIRE(std::get<update_many_t>(record.data).match_.query->group() == expression_group::compare);
        auto match = reinterpret_cast<const compare_expression_ptr&>(std::get<update_many_t>(record.data).match_.query);
        REQUIRE(match->type() == compare_type::eq);
        REQUIRE(match->key() == components::expressions::key_t{"count"});
        REQUIRE(match->value() == core::parameter_id_t{1});
        REQUIRE(std::get<update_many_t>(record.data).parameters().parameters.size() == 1);
        REQUIRE(get_parameter(&std::get<update_many_t>(record.data).parameters(), core::parameter_id_t{1}).as_int() ==
                num);
        auto doc = std::get<update_many_t>(record.data).update_;
        REQUIRE(doc->get_dict("$set")->get_long("count") == num + 10);
        REQUIRE(std::get<update_many_t>(record.data).upsert_ == (num % 2 == 0));
        index = test_wal.wal->test_next_record(index);
    }
}

TEST_CASE("test find start record") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/find_start_record", &resource);
    test_insert_one(test_wal.wal);

    std::size_t start_index;
    REQUIRE(test_wal.wal->test_find_start_record(services::wal::id_t(1), start_index));
    REQUIRE(test_wal.wal->test_find_start_record(services::wal::id_t(5), start_index));
    REQUIRE_FALSE(test_wal.wal->test_find_start_record(services::wal::id_t(6), start_index));
    REQUIRE_FALSE(test_wal.wal->test_find_start_record(services::wal::id_t(0), start_index));
}

TEST_CASE("test read id") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/read_id", &resource);
    test_insert_one(test_wal.wal);

    std::size_t index = 0;
    for (int num = 1; num <= 5; ++num) {
        REQUIRE(test_wal.wal->test_read_id(index) == services::wal::id_t(num));
        index = test_wal.wal->test_next_record(index);
    }
    REQUIRE(test_wal.wal->test_read_id(index) == services::wal::id_t(0));
}

TEST_CASE("test read record") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto test_wal = create_test_wal("/tmp/wal/read_record", &resource);
    test_insert_one(test_wal.wal);

    std::size_t index = 0;
    for (int num = 1; num <= 5; ++num) {
        auto record = test_wal.wal->test_read_record(index);
        REQUIRE(record.type == statement_type::insert_one);
        REQUIRE(std::get<insert_one_t>(record.data).database_ == database_name);
        REQUIRE(std::get<insert_one_t>(record.data).collection_ == collection_name);
        auto doc = std::get<insert_one_t>(record.data).document_;
        REQUIRE(doc->get_string("/_id") == gen_id(num, &resource));
        REQUIRE(doc->get_long("/count") == num);
        REQUIRE(doc->get_string("/countStr") == std::pmr::string(std::to_string(num), &resource));
        index = test_wal.wal->test_next_record(index);
    }
    REQUIRE(test_wal.wal->test_read_record(index).type == statement_type::unused);
}

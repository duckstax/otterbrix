#include <catch2/catch.hpp>

#include <crc32c/crc32c.h>
#include <actor-zeta.hpp>
#include <log/log.hpp>
#include <wal/wal.hpp>

#include <msgpack.hpp>
#include <string>

#include "manager_wal_replicate.hpp"
#include "wal.hpp"
#include <components/protocol/insert_many.hpp>

#include <components/tests/generaty.hpp>
#include <components/document/document_view.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>

using namespace services::wal;

TEST_CASE("insert one test") {
    static auto log = initialization_logger("duck_charmer", "/tmp/docker_logs/");
    log.set_level(log_t::level::trace);
    auto* scheduler_( new core::non_thread_scheduler::scheduler_test_t(1, 1));
    actor_zeta::detail::pmr::memory_resource *resource = actor_zeta::detail::pmr::get_default_resource();
    auto manager = actor_zeta::spawn_supervisor<manager_wal_replicate_t>(resource,scheduler_,boost::filesystem::current_path(), log, 1, 1000);
    auto allocate_byte = sizeof(wal_replicate_t);
    auto allocate_byte_alignof = alignof(wal_replicate_t);
    void* buffer = manager->resource()->allocate(allocate_byte, allocate_byte_alignof);
    auto* wal = new (buffer) wal_replicate_t(manager.get(), log, boost::filesystem::current_path());

    const std::string database = "test_database";
    const std::string collection = "test_collection";

    for (int num = 1; num <= 5; ++num) {
        auto document = gen_doc(num);
        insert_one_t data(database, collection, std::move(document));
        auto session = components::session::session_id_t();
        auto address = actor_zeta::base::address_t::address_t::empty_address();
        wal->insert_one(session, address, data);
    }

    std::size_t read_index = 0;
    for (int num = 1; num <= 5; ++num) {
        wal_entry_t<insert_one_t> entry;

        entry.size_ = wal->read_size(read_index);

        auto start = read_index + sizeof(size_tt);
        auto finish = read_index + sizeof(size_tt) + entry.size_ + sizeof(crc32_t);
        auto output = wal->read(start, finish);

        auto crc32_index = entry.size_;
        crc32_t crc32 = crc32c::Crc32c(output.data(), crc32_index);

        unpack(output, entry);
        entry.crc32_ = read_crc32(output, entry.size_);
        scheduler_->run();
        REQUIRE(entry.crc32_ == crc32);
        REQUIRE(entry.entry_.database_ == database);
        REQUIRE(entry.entry_.collection_ == collection);
        document_view_t view(entry.entry_.document_);
        REQUIRE(view.get_string("_id") == gen_id(num));
        REQUIRE(view.get_long("count") == num);
        REQUIRE(view.get_string("countStr") == std::to_string(num));

        read_index = finish;
    }
}

TEST_CASE("insert many empty test") {
    static auto log = initialization_logger("duck_charmer", "/tmp/docker_logs/");
    log.set_level(log_t::level::trace);
    auto* scheduler_( new core::non_thread_scheduler::scheduler_test_t(1, 1));
    actor_zeta::detail::pmr::memory_resource *resource = actor_zeta::detail::pmr::get_default_resource();
    auto manager = actor_zeta::spawn_supervisor<manager_wal_replicate_t>(resource,scheduler_,boost::filesystem::current_path(), log, 1, 1000);
    auto allocate_byte = sizeof(wal_replicate_t);
    auto allocate_byte_alignof = alignof(wal_replicate_t);
    void* buffer = manager->resource()->allocate(allocate_byte, allocate_byte_alignof);
    auto* wal = new (buffer) wal_replicate_t(manager.get(), log, boost::filesystem::current_path());

    const std::string database = "test_database";
    const std::string collection = "test_collection";

    std::list<components::document::document_ptr> documents;
    insert_many_t data(database, collection, std::move(documents));

    auto session = components::session::session_id_t();
    auto address = actor_zeta::base::address_t::address_t::empty_address();
    wal->insert_many(session, address, data);

    wal_entry_t<insert_many_t> entry;

    entry.size_ = wal->read_size(0);

    auto start = sizeof(size_tt);
    auto finish = sizeof(size_tt) + entry.size_ + sizeof(crc32_t);
    auto output = wal->read(start, finish);

    auto crc32_index = entry.size_;
    crc32_t crc32 = crc32c::Crc32c(output.data(), crc32_index);

    unpack(output, entry);
    entry.crc32_ = read_crc32(output, entry.size_);
    scheduler_->run();
    REQUIRE(entry.crc32_ == crc32);
}

TEST_CASE("insert many test") {
    static auto log = initialization_logger("duck_charmer", "/tmp/docker_logs/");
    log.set_level(log_t::level::trace);
    auto* scheduler_( new core::non_thread_scheduler::scheduler_test_t(1, 1));
    actor_zeta::detail::pmr::memory_resource *resource = actor_zeta::detail::pmr::get_default_resource();
    auto manager = actor_zeta::spawn_supervisor<manager_wal_replicate_t>(resource,scheduler_,boost::filesystem::current_path(), log, 1, 1000);
    auto allocate_byte = sizeof(wal_replicate_t);
    auto allocate_byte_alignof = alignof(wal_replicate_t);
    void* buffer = manager->resource()->allocate(allocate_byte, allocate_byte_alignof);
    auto* wal = new (buffer) wal_replicate_t(manager.get(), log, boost::filesystem::current_path());

    const std::string database = "test_database";
    const std::string collection = "test_collection";

    for (int i = 0; i <= 3; ++i) {
        std::list<components::document::document_ptr> documents;
        for (int num = 1; num <= 5; ++num) {
            documents.push_back(gen_doc(num));
        }
        insert_many_t data(database, collection, std::move(documents));
        auto session = components::session::session_id_t();
        auto address = actor_zeta::base::address_t::address_t::empty_address();
        wal->insert_many(session, address, data);
    }

    std::size_t read_index = 0;
    for (int i = 0; i <= 3; ++i) {
        wal_entry_t<insert_many_t> entry;

        entry.size_ = wal->read_size(read_index);

        auto start = read_index + sizeof(size_tt);
        auto finish = read_index + sizeof(size_tt) + entry.size_ + sizeof(crc32_t);
        auto output = wal->read(start, finish);

        auto crc32_index = entry.size_;
        crc32_t crc32 = crc32c::Crc32c(output.data(), crc32_index);

        unpack(output, entry);
        entry.crc32_ = read_crc32(output, entry.size_);
        scheduler_->run();
        REQUIRE(entry.crc32_ == crc32);
        REQUIRE(entry.entry_.database_ == database);
        REQUIRE(entry.entry_.collection_ == collection);
        REQUIRE(entry.entry_.documents_.size() == 5);
        int num = 0;
        for (const auto &doc : entry.entry_.documents_) {
            ++num;
            document_view_t view(doc);
            REQUIRE(view.get_string("_id") == gen_id(num));
            REQUIRE(view.get_long("count") == num);
            REQUIRE(view.get_string("countStr") == std::to_string(num));
        }

        read_index = finish;
    }
}
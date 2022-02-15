#include <catch2/catch.hpp>

#include <goblin-engineer/core.hpp>
#include <log/log.hpp>
#include <wal/wal.hpp>
#include <crc32c/crc32c.h>

#include <msgpack.hpp>
#include <string>

#include <components/protocol/insert_many.hpp>
#include "manager_wal_replicate.hpp"
#include "wal.hpp"

TEST_CASE("insert_many_t test") {
    static auto log = initialization_logger("duck_charmer", "/tmp/docker_logs/");
    log.set_level(log_t::level::trace);
    auto manager = goblin_engineer::make_manager_service<manager_wal_replicate_t>(log, 1, 1000);
    auto allocate_byte = sizeof(wal_replicate_t);
    auto allocate_byte_alignof = alignof(wal_replicate_t);
    void* buffer = manager->resource()->allocate(allocate_byte, allocate_byte_alignof);
    auto* wal = new (buffer) wal_replicate_t(nullptr, log, boost::filesystem::current_path());

    const std::string database = "test_database";
    const std::string collection = "test_collection";
    std::list<components::document::document_t> documents ;
    insert_many_t data(database,collection,documents) ;

    wal->insert_many(data);
/*
    wal_entry_t entry;

    entry.size_ = wal->read_size(0);

    auto start = sizeof(size_tt);
    auto finish = sizeof(size_tt) + entry.size_ + sizeof(crc32_t);
    auto output = wal->read(start, finish);

    auto crc32_index = entry.size_;
    crc32_t crc32 = crc32c::Crc32c(output.data(),crc32_index);
    unpack_v2(output,entry);

    REQUIRE(entry.crc32_ == crc32);

    msgpack::unpacked msg_1;
    msgpack::unpack(msg_1, entry.entry_.payload_.data(), entry.entry_.payload_.size());
    const auto& o_1 = msg_1.get();
*/
}
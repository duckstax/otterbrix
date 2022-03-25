#include <catch2/catch.hpp>

#include <goblin-engineer/core.hpp>
#include <log/log.hpp>
#include <wal/wal.hpp>

#include <msgpack.hpp>
#include <string>

#include <crc32c/crc32c.h>

TEST_CASE("pack and unpack") {

    const std::string database = "test_database";
    const std::string collection = "test_collection";

    std::list<components::document::document_ptr> documents ;
    insert_many_t data(database,collection,std::move(documents));

    const crc32_t  last_crc32 = 42;
    const log_number_t log_number = 21;

    buffer_t  buffer;

    pack(buffer,last_crc32,log_number,data);
    wal_entry_t<insert_many_t> entry;
    entry.size_= read_size_impl(buffer, 0);

    auto start = sizeof(size_tt);
    auto finish = sizeof(size_tt) + entry.size_ + sizeof(crc32_t);
    auto storage = read_payload(buffer, start, finish);

    unpack<insert_many_t>(storage,entry);
    entry.crc32_ = read_crc32(storage, entry.size_);

    REQUIRE(entry.last_crc32_ == last_crc32);
    REQUIRE(entry.type_ == statement_type::insert_many);
    REQUIRE(entry.log_number_ == log_number);

}
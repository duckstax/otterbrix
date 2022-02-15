#include <catch2/catch.hpp>

#include <goblin-engineer/core.hpp>
#include <log/log.hpp>
#include <wal/wal.hpp>

#include <msgpack.hpp>
#include <string>

#include <crc32c/crc32c.h>

struct get_t {
    statement_type type() const {
        return statement_type::create_collection;
    }
    get_t()  =default;
    get_t(uint32_t f, const std::string& k)
        : flags(f)
        , key(k) {}
    uint32_t flags;
    std::string key;
    MSGPACK_DEFINE(flags, key);
};

TEST_CASE("2 level serilizate to msgpack") {
    msgpack::sbuffer input;
    get_t req;
    req.flags = 0;
    req.key = "key0";

    const crc32_t  last_crc32 = 42;
    const log_number_t log_number = 21;

    buffer_t  buffer;
    pack(buffer,last_crc32,log_number,req) ;

    wal_entry_t<get_t> entry;

    entry.size_= read_size_impl(buffer, 0);

    unpack(buffer,entry);

    REQUIRE(entry.last_crc32_ == last_crc32);
   /// REQUIRE(entry.type_ == statement_type::create_collection);
    REQUIRE(entry.log_number_ == log_number);

    REQUIRE(entry.entry_.key == "key0");
    REQUIRE(entry.entry_.flags == 0);
}
/*
TEST_CASE("msgpack to bin") {
    msgpack::sbuffer input;
    get_t req;
    req.flags = 0;
    req.key = "key0";
    msgpack::pack(input, req);
    buffer_t binary_input;
    std::copy(input.data(), input.data() + input.size(), std::back_inserter(binary_input));
    entry_t entry(42, Type::create_collection, 21, binary_input);

    buffer_t storage_;
    crc32_t crc32 = pack(storage_, entry);

    wal_entry_t wal_entry;
    unpack(storage_, wal_entry);

    REQUIRE(wal_entry.entry_.last_crc32_ == 42);
    REQUIRE(wal_entry.entry_.type_ == Type::create_collection);
    REQUIRE(wal_entry.entry_.log_number_ == 21);
    REQUIRE(wal_entry.crc32_ == crc32);
    REQUIRE(wal_entry.size_ == 25);

    msgpack::unpacked msg_1;
    msgpack::unpack(msg_1, reinterpret_cast<char*>(wal_entry.entry_.payload_.data()), wal_entry.entry_.payload_.size());
    const auto& o_1 = msg_1.get();
    get_t req_1;
    o_1.convert(req_1);

    REQUIRE(req_1.key == "key0");
    REQUIRE(req_1.flags == 0);
}*/
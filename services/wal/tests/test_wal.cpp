#include <catch2/catch.hpp>

#include <goblin-engineer/core.hpp>
#include <log/log.hpp>
#include <wal/wal.hpp>

#include <iostream>
#include <msgpack.hpp>
#include <sstream>
#include <string>

struct get_t {
    get_t() {}
    get_t(uint32_t f, const std::string& k)
        : flags(f)
        , key(k) {}
    uint32_t flags;
    std::string key;
    MSGPACK_DEFINE(flags, key);
};

TEST_CASE("event to byte array ") {
    msgpack::sbuffer input;
    get_t req;
    req.flags = 0;
    req.key = "key0";
    msgpack::pack(input, req);
    buffer_t bynary_imput;
    std::copy(input.data(), input.data() + input.size(), std::back_inserter(bynary_imput));

    buffer_t bynary_output;
    bynary_output.resize(  heer_size + bynary_imput.size());
    log_number_t log_number = 42;
    Type type = Type::create_collection;
    pack(type, bynary_imput, log_number, bynary_output);
    wal_entry_t entry;
    unpack(bynary_output, entry);
    REQUIRE(entry.log_number_ == log_number);
    REQUIRE(entry.type_ == type);

    msgpack::object_handle oh = msgpack::unpack(reinterpret_cast<char*>(entry.payload_.data()), entry.payload_.size());
    msgpack::object o = oh.get();
    get_t req_;
    o.convert(req);
    REQUIRE(req_.flags == 0);
    REQUIRE(req_.key == "key0");
}

TEST_CASE("wal add event") {
    static auto log = initialization_logger("duck_charmer", "/tmp/docker_logs/");
    log.set_level(log_t::level::trace);

    auto manager = goblin_engineer::make_manager_service<wdr_t>(log, 1, 1000);
    auto allocate_byte = sizeof(wal_t);
    auto allocate_byte_alignof = alignof(wal_t);
    void* buffer = manager->resource()->allocate(allocate_byte, allocate_byte_alignof);
    auto* wal = new (buffer) wal_t(nullptr, log, std::filesystem::current_path());
    buffer_t tmp(10, buffer_element_t (1));
    wal->add_event(Type::create_collection, tmp);
    ////REQUIRE(collection->find_test(parse_find_condition("{\"new_array.0.0\": {\"$eq\": \"NoName\"}}"))->size() == 1);
}

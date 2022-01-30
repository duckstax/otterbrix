#include <catch2/catch.hpp>

#include <goblin-engineer/core.hpp>
#include <log/log.hpp>
#include <wal/wal.hpp>

#include <msgpack.hpp>
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

TEST_CASE("2 level serilizate") {
    msgpack::sbuffer input;
    get_t req;
    req.flags = 0;
    req.key = "key0";
    msgpack::pack(input, req);
    buffer_t bynary_imput;
    std::copy(input.data(), input.data() + input.size(), std::back_inserter(bynary_imput));
    entry_t entry(42,Type::create_collection,21,bynary_imput);
    msgpack::sbuffer input_1;
    msgpack::pack(input_1,  entry);
    msgpack::unpacked msg;
    msgpack::unpack(msg,input_1.data(), input_1.size());
    const auto& o = msg.get();
    auto  output = o.as<entry_t>();
    REQUIRE(output.last_crc32_== 42);
    REQUIRE(output.type_ == Type::create_collection);
    REQUIRE(output.log_number_ == 21);

    msgpack::unpacked msg_1;
    msgpack::unpack(msg_1,reinterpret_cast<char*>(output.payload_.data()), output.payload_.size());

    const auto& o_1 = msg_1.get();
   get_t req_1;
    o_1.convert(req_1);
   REQUIRE(req_1.key == "key0");
    REQUIRE(req_1.flags == 0);

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
}

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

TEST_CASE("2 level serilizate to msgpack") {
    msgpack::sbuffer input;
    get_t req;
    req.flags = 0;
    req.key = "key0";
    msgpack::pack(input, req);
    buffer_t binary_input;
    std::copy(input.data(), input.data() + input.size(), std::back_inserter(binary_input));

    entry_t entry(42, Type::create_collection, 21, binary_input);
    msgpack::sbuffer input_1;
    msgpack::pack(input_1, entry);

    msgpack::unpacked msg;
    msgpack::unpack(msg, input_1.data(), input_1.size());
    const auto& o = msg.get();
    auto output = o.as<entry_t>();

    REQUIRE(output.last_crc32_ == 42);
    REQUIRE(output.type_ == Type::create_collection);
    REQUIRE(output.log_number_ == 21);

    msgpack::unpacked msg_1;
    msgpack::unpack(msg_1, reinterpret_cast<char*>(output.payload_.data()), output.payload_.size());
    const auto& o_1 = msg_1.get();
    get_t req_1;
    o_1.convert(req_1);

    REQUIRE(req_1.key == "key0");
    REQUIRE(req_1.flags == 0);
}

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
}

TEST_CASE("wal add event") {
    static auto log = initialization_logger("duck_charmer", "/tmp/docker_logs/");
    log.set_level(log_t::level::trace);
    auto manager = goblin_engineer::make_manager_service<wdr_t>(log, 1, 1000);
    auto allocate_byte = sizeof(wal_t);
    auto allocate_byte_alignof = alignof(wal_t);
    void* buffer = manager->resource()->allocate(allocate_byte, allocate_byte_alignof);
    auto* wal = new (buffer) wal_t(nullptr, log, boost::filesystem::current_path());

    msgpack::sbuffer input;
    get_t req;
    req.flags = 0;
    req.key = "key0";
    msgpack::pack(input, req);
    buffer_t binary_input;
    std::copy(input.data(), input.data() + input.size(), std::back_inserter(binary_input));

    wal->add_event(Type::create_collection, binary_input);
    wal_entry_t entry;
    entry.size_ = wal->read_size(0);
    auto start = sizeof(size_tt);
    auto finish = sizeof(size_tt) + entry.size_ + sizeof(crc32_t);
    auto output = wal->read(start, finish);
    unpack_v2(output,entry);

    msgpack::unpacked msg_1;
    msgpack::unpack(msg_1, reinterpret_cast<char*>(entry.entry_.payload_.data()), entry.entry_.payload_.size());
    const auto& o_1 = msg_1.get();
    get_t req_1;
    o_1.convert(req_1);

    REQUIRE(req_1.key == "key0");
    REQUIRE(req_1.flags == 0);

}
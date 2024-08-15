#include <catch2/catch.hpp>

#include <components/document/document.hpp>
#include <components/document/msgpack/msgpack_encoder.hpp>
#include <components/tests/generaty.hpp>
#include <core/b_plus_tree/msgpack_reader/msgpack_reader.hpp>
#include <msgpack.hpp>

using namespace core::b_plus_tree;
using namespace components::types;

TEST_CASE("msgpack_reader") {
    INFO("native packed document") {
        auto resource = std::pmr::synchronized_pool_resource();
        constexpr int num = 10;
        auto doc1 = gen_doc(num, &resource);
        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, doc1);

        msgpack::unpacked msg;
        msgpack::unpack(msg, sbuf.data(), sbuf.size());
        auto index = get_field(msg.get(), "/_id");

        REQUIRE(doc1->get_string("/_id") == get_field(msg.get(), "/_id").value<physical_type::STRING>());
        REQUIRE(doc1->get_long("/count") == get_field(msg.get(), "/count").value<physical_type::UINT64>());
        REQUIRE(doc1->get_string("/countStr") == get_field(msg.get(), "/countStr").value<physical_type::STRING>());
        REQUIRE(doc1->get_double("/countDouble") ==
                get_field(msg.get(), "/countDouble").value<physical_type::DOUBLE>());
        REQUIRE(doc1->get_bool("/countBool") == get_field(msg.get(), "/countBool"));
        REQUIRE(doc1->get_dict("/null") == get_field(msg.get(), "/null").value<physical_type::NA>());
        for (int i = 0; i < doc1->get_array("/countArray")->count(); i++) {
            std::string json_ptr = "/countArray/" + std::to_string(i);
            REQUIRE(doc1->get_long(json_ptr) == get_field(msg.get(), json_ptr).value<physical_type::UINT64>());
        }
        REQUIRE(doc1->get_bool("/countDict/odd") == get_field(msg.get(), "/countDict/odd"));
        REQUIRE(doc1->get_bool("/countDict/even") == get_field(msg.get(), "/countDict/even"));
    }
}
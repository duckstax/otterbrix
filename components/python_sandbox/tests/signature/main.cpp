#include <memory>

#include <botan/hex.h>
#include <botan/mac.h>

#include <nlohmann/json.hpp>

#include <gtest/gtest.h>
#include "detail/signature_generator.hpp"

using namespace nlohmann;

TEST(signature, generate) {
    json header = json::object();
    json parent_header = json::object();
    json metadata = json::object();
    json content = json::object();
    std::string signature_key("1qaz");

    auto mac = Botan::MessageAuthenticationCode::create("HMAC(SHA-256)");
    mac->set_key(std::vector<std::uint8_t>(signature_key.begin(), signature_key.end()));
    mac->start();
    mac->update(header.dump());
    mac->update(parent_header.dump());
    mac->update(metadata.dump());
    mac->update(content.dump());
    auto botan_sig = Botan::hex_encode(mac->final(), false);

    signature_generator sg("hmac-sha256",signature_key);
    auto sig = sg.sign(header.dump(),parent_header.dump(),metadata.dump(),content.dump());
    ASSERT_EQ(botan_sig,sig);
    ASSERT_TRUE(sg.verify(header.dump(),parent_header.dump(),metadata.dump(),content.dump(),sig));
}


int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
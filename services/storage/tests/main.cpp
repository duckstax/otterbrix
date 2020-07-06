#include <gtest/gtest.h>

#include <string>

#include <services/storage/hash_storage.hpp>
#include <services/storage/storage_engine.hpp>

constexpr static char* name = "test_name";

TEST(init, memory_hash_storage) {
    auto storage = std::make_unique<services::memory_hash_storage>();
    services::temporary_buffer_storage tbs_input;
    tbs_input.emplace_back("1", std::make_unique<services::buffer_tt>("2"));
    storage->put(tbs_input);
    services::temporary_buffer_storage tbs_out;
    tbs_out.emplace_back("1", std::make_unique<services::buffer_tt>());
    storage->get(tbs_out);
    auto element = (*(tbs_out.at(0).second));
    ASSERT_EQ(element,std::string("2"));
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
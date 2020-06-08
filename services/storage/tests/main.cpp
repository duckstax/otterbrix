#include "gtest/gtest.h"
#include <services/storage/hash_storage.hpp>
#include <services/storage/storage_engine.hpp>

constexpr static char* name = "test_name";

TEST(register_storage_storage, storage_engine) {
    services::storage_engine se;
    se.register_storage(name, std::make_unique<services::memory_hash_storage>());
    auto& storage_type = se.get_store(name);
}

TEST(init, memory_hash_storage) {
    auto storage = std::make_unique<services::memory_hash_storage>();
    services::temporary_buffer_storage tbs;
    tbs.emplace_back("1111111111", std::make_unique<services::buffer_tt>("111111111"));
    storage->put(tbs);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
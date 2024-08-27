#include "test_config.hpp"
#include <catch2/catch.hpp>

TEST_CASE("integration::cpp::test_instances") {
    auto config_works_1 = test_create_config("/tmp/test_instances/1");
    auto config_works_2 = test_create_config("/tmp/test_instances/2");
    auto config_works_3 = test_create_config(); // default config
    auto config_failes_1 = test_create_config("/tmp/test_instances/1");
    auto config_failes_2 = test_create_config(); // default config

    INFO("unique directories") {
        test_spaces space_1(config_works_1);
        test_spaces space_2(config_works_2);
        test_spaces space_3(config_works_3);

        try {
            test_spaces space(config_failes_1);
            REQUIRE(false);
        } catch (...) {
            REQUIRE(true);
        }

        try {
            test_spaces space(config_failes_2);
            REQUIRE(false);
        } catch (...) {
            REQUIRE(true);
        }
    }

    INFO("directories references deleted properly") {
        // basically run the same setup again
        test_spaces space_1(config_works_1);
        test_spaces space_2(config_works_2);
        test_spaces space_3(config_works_3);

        try {
            test_spaces space(config_failes_1);
            REQUIRE(false);
        } catch (...) {
            REQUIRE(true);
        }

        try {
            test_spaces space(config_failes_2);
            REQUIRE(false);
        } catch (...) {
            REQUIRE(true);
        }
    }
}

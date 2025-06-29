project(test_catalog)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        utils.cpp
        test_catalog.cpp
        test_transactions.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

add_executable(${PROJECT_NAME}_trie trie_tests.cpp ../trie/trie_iterator.cpp ../trie/versioned_trie.cpp)

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::catalog
        Catch2::Catch2
        magic_enum::magic_enum
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})

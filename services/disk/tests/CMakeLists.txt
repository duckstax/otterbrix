project(test_disk)

set(${PROJECT_NAME}_SOURCES
    main.cpp
    test_disk_database.cpp
    test_disk_collection.cpp
    test_disk_document.cpp
    test_metadata.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    rocketjoe::document
    rocketjoe::disk
    rocketjoe::test_generaty
    CONAN_PKG::rocksdb
    CONAN_PKG::boost
    CONAN_PKG::catch2
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})

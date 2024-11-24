project(test_new_document_base)

set( ${PROJECT_NAME}_SOURCES
        test_document_json.cpp
        test_document_t.cpp
        test_document_pack.cpp
)

find_package( Threads )

add_executable(${PROJECT_NAME} main.cpp  ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::document
        otterbrix::test_generaty
        Boost::boost
        Catch2::Catch2
        msgpackc-cxx
        ${CMAKE_THREAD_LIBS_INIT}
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
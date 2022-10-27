project(test_document_base)

set( ${PROJECT_NAME}_SOURCES
	    test_mutable.cpp
	    test_shared_keys.cpp
	    test_support.cpp
	    test_value.cpp
		test_pack_msgpack.cpp
		test_document_id.cpp
		test_document_json.cpp
		test_document_view.cpp
)

find_package( Threads )

add_executable(${PROJECT_NAME} main.cpp  ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        rocketjoe::document
		rocketjoe::test_generaty
		CONAN_PKG::catch2
		${CMAKE_THREAD_LIBS_INIT}
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})

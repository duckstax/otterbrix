project(document)

file(GLOB_RECURSE DOCUMENT_SOURCES "*.cpp")

add_library(
        document_library
        ${DOCUMENT_SOURCES}
)

target_include_directories(document_library PUBLIC "${CMAKE_SOURCE_DIR}/")

target_link_libraries(
        document_library
        CONAN_PKG::boost
        CONAN_PKG::abseil
        CONAN_PKG::msgpack-cxx
)
option(CLANG_CODE_COVERAGE "Enable code coverage metrics in Clang" OFF)

if (CLANG_CODE_COVERAGE)
    message(STATUS "Code coverage metrics enabled for build")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
endif ()
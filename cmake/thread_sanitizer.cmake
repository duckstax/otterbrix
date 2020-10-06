option(ADDRESS_SANITIZER "Enable Clang AddressSanitizer" OFF)

if (ADDRESS_SANITIZER)
    message(STATUS "ThreadSanitizer enabled for build")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O1 -fno-omit-frame-pointer -fsanitize=thread")
endif ()
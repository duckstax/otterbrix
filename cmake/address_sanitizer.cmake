message(STATUS "AddressSanitizer enabled for build")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O1 -fno-omit-frame-pointer -fsanitize=address")
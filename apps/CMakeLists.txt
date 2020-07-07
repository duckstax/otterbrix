include_directories(${CMAKE_SOURCE_DIR})

if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
    add_subdirectory(debug-worker)
endif()

add_subdirectory(resource-hub)
add_subdirectory(rocketjoe)
add_subdirectory(rocketjoe_engine)
add_subdirectory(rocketjoe_kernel)
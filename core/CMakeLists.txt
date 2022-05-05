include_directories(${CMAKE_SOURCE_DIR})

add_subdirectory(spinlock)

if (DEV_MODE)
#    add_subdirectory(tests)
endif()

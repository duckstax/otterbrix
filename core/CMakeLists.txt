include_directories(${CMAKE_SOURCE_DIR})

add_subdirectory(spinlock)
add_subdirectory(non_thread_scheduler)
add_subdirectory(file)

if (DEV_MODE)
#    add_subdirectory(tests)
endif()

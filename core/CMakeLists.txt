include_directories(${CMAKE_SOURCE_DIR})


if (WASM_BUILD)

else ()
    add_subdirectory(assert)
    add_subdirectory(spinlock)
    add_subdirectory(non_thread_scheduler)
    add_subdirectory(file)
endif ()

if (DEV_MODE)
    add_subdirectory(tests)
endif ()

add_library(actor actor.cpp)
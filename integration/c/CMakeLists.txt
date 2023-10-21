project(c_ottergon C CXX)


set(${PROJECT_NAME}_sources
        main.cpp
)

set(ROCKETJOE_LIBS
        rocketjoe::log
        rocketjoe::memory_storage
        rocketjoe::collection
        rocketjoe::cursor
        rocketjoe::session
        rocketjoe::document
        rocketjoe::dispatcher
        rocketjoe::wal
        rocketjoe::disk
        rocketjoe::locks
        rocketjoe::sql
        rocketjoe::result
)


include_directories(
        ${PROJECT_SOURCE_DIR}
)

add_library(${PROJECT_NAME} SHARED ${${PROJECT_NAME}_sources})


target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        cpp_ottergon
        ${ROCKETJOE_LIBS}
)

#set_target_properties(${PROJECT_NAME} PROPERTIES OUTPUT_NAME "ottergon")

if (DEV_MODE)
    add_subdirectory(test)
endif()

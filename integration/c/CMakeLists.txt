project(c_ottergon C CXX)


set(${PROJECT_NAME}_sources
        main.cpp
)

set(ottergon_LIBS
        ottergon::log
        ottergon::memory_storage
        ottergon::collection
        ottergon::cursor
        ottergon::session
        ottergon::document
        ottergon::dispatcher
        ottergon::wal
        ottergon::disk
        ottergon::locks
        ottergon::sql
)


include_directories(
        ${PROJECT_SOURCE_DIR}
)

add_library(${PROJECT_NAME} SHARED ${${PROJECT_NAME}_sources})


target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        cpp_ottergon
        ${ottergon_LIBS}
)

#set_target_properties(${PROJECT_NAME} PROPERTIES OUTPUT_NAME "ottergon")

if (DEV_MODE)
    add_subdirectory(test)
endif()

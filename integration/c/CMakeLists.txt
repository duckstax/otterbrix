project(c_otterbrix C CXX)


set(${PROJECT_NAME}_sources
        main.cpp
)

<<<<<<< HEAD
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
=======
set(otterbrix_LIBS
        otterbrix::log
        otterbrix::memory_storage
        otterbrix::collection
        otterbrix::cursor
        otterbrix::session
        otterbrix::document
        otterbrix::dispatcher
        otterbrix::wal
        otterbrix::disk
        otterbrix::locks
        otterbrix::sql
        otterbrix::result
>>>>>>> upstream/develop
)


include_directories(
        ${PROJECT_SOURCE_DIR}
)

add_library(${PROJECT_NAME} SHARED ${${PROJECT_NAME}_sources})


target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        cpp_otterbrix
        ${otterbrix_LIBS}
)

#set_target_properties(${PROJECT_NAME} PROPERTIES OUTPUT_NAME "otterbrix")

if (DEV_MODE)
    add_subdirectory(test)
endif()

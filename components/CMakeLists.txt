include_directories(${CMAKE_SOURCE_DIR})

add_subdirectory(document)

if(WASM_BUILD)

else()
    add_subdirectory(log)
    add_subdirectory(oid)
    add_subdirectory(ql)
    add_subdirectory(index)
    add_subdirectory(cursor)
    add_subdirectory(session)
    add_subdirectory(statistic)
    add_subdirectory(parser)
    add_subdirectory(serialize)
    add_subdirectory(wasm_runner)
endif()

if (DEV_MODE)
    add_subdirectory(tests)
endif()

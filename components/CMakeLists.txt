include_directories(${CMAKE_SOURCE_DIR})

add_subdirectory(document)

if(WASM_BUILD)

else()
    add_subdirectory(cursor)
    add_subdirectory(dataframe)
    add_subdirectory(expressions)
    add_subdirectory(index)
    add_subdirectory(log)
    add_subdirectory(logical_plan)
    add_subdirectory(oid)
    add_subdirectory(ql)
    add_subdirectory(translator)
    add_subdirectory(session)
    add_subdirectory(wasm_runner)
endif()

if (DEV_MODE)
    add_subdirectory(tests)
endif()

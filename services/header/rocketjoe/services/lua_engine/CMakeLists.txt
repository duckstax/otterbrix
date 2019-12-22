set(CURRENT_TARGET lua_engine)

list(APPEND CURRENT_TARGET_HEADERS
        lua_engine.hpp
        device.hpp
        lua_sandbox.hpp

)

list(APPEND CURRENT_TARGET_SOURCES
        lua_engine.cpp
        lua_sandbox.cpp
)

add_library(rocketjoe_${CURRENT_TARGET} STATIC
            ${CURRENT_TARGET_HEADERS}
            ${CURRENT_TARGET_SOURCES}
)


set(LUA_PC_CFG "lua;lua5.3;lua5.2;lua5.1" CACHE STRING "pkg-config Lua configuration file (or files, separated by ;)")

find_package(PkgConfig)
if(PKG_CONFIG_EXECUTABLE)
    pkg_search_module(LUA REQUIRED ${LUA_PC_CFG})
else()
    if(NOT LUA_INCLUDE_DIR)
        message(SEND_ERROR "LUA_INCLUDE_DIR is required if no PkgConfig is available!")
    endif()
    if(NOT LUA_LIBRARIES)
        message(SEND_ERROR "LUA_LIBRARIES is required if no PkgConfig is available!")
    endif()
endif()

find_package(Lua 5.3)

if(${LUA_INCLUDE_DIR})
    if(NOT EXISTS ${LUA_INCLUDE_DIR}/lua.h)
        message(SEND_ERROR "Can't find lua.h in ${LUA_INCLUDE_DIR}")
    endif()
endif()

include_directories(${LUA_INCLUDE_DIRS})

include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/thirdparty/sol2)

add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        rocketjoe_${CURRENT_TARGET} PRIVATE
        rocketjoe::network
        rocketjoe::http_server
        goblin-engineer
        ${LUA_LIBRARIES}
)

target_include_directories(rocketjoe_${CURRENT_TARGET} PUBLIC "../../../../lua_engine/header" "../../../../..")

install(TARGETS
        rocketjoe_${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

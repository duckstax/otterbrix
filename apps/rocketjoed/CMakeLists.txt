set(CURRENT_TARGET rocketjoed)

add_executable(${CURRENT_TARGET}  rocketjoed.cpp ../../lua/web.lua)

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        yaml-cpp
        goblin-engineer
        rocketjoe::lua_engine
        rocketjoe::ws_server
        rocketjoe::http_server
)


install(TARGETS
        ${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
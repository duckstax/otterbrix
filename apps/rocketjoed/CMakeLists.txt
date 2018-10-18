set(CURRENT_TARGET rocketjoed)

add_executable(${CURRENT_TARGET}  rocketjoed.cpp )

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE

        rocketjoe::lua_engine
        rocketjoe::ws_server
        rocketjoe::http_server
        rocketjoe::database
        rocketjoe::object_storage

        yaml-cpp
        goblin-engineer
)


install(TARGETS
        ${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
set(CURRENT_TARGET rocketjoed)

add_executable(${CURRENT_TARGET}  rocketjoed.cpp init_service.hpp init_service.cpp configuration.cpp configuration.hpp)

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE

        rocketjoe::lua_engine
        rocketjoe::ws
        rocketjoe::http
        rocketjoe::database
        rocketjoe::object_storage
        rocketjoe::flat_cache
        rocketjoe::router

        cxxopts
        yaml-cpp
        goblin-engineer
        dl
)


install(TARGETS
        ${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
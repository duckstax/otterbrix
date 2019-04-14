set(CURRENT_TARGET rocketjoed)

set(${CURRENT_TARGET}_headers
        init_service.hpp
        configuration.hpp
)

set(${CURRENT_TARGET}_sources
        rocketjoed.cpp
        init_service.cpp
        configuration.cpp
)

#############################################################################################
#cxxopts

include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/thirdparty/cxxopts/include)

#cxxopts
#############################################################################################


add_executable(${CURRENT_TARGET}  ${${CURRENT_TARGET}_sources} ${${CURRENT_TARGET}_headers} )

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        ${CONAN_LIBS}
        rocketjoe::lua_engine
        rocketjoe::ws
        rocketjoe::http
        rocketjoe::object_storage
        rocketjoe::flat_cache
        rocketjoe::router

        cxxopts
        goblin-engineer
        dl
)


install(TARGETS
        ${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
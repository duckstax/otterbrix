set(CURRENT_TARGET rocketjoed)

set(${CURRENT_TARGET}_headers
        configuration.hpp
        init_service.hpp
)

set(${CURRENT_TARGET}_sources
        configuration.cpp
        init_service.cpp
        rocketjoed.cpp
)

#############################################################################################
#cxxopts
include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/thirdparty/cxxopts/include)
#cxxopts
#############################################################################################


add_executable(${CURRENT_TARGET}  ${${CURRENT_TARGET}_sources} ${${CURRENT_TARGET}_headers} )
target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        goblin-engineer
        rocketjoe::http_server
        rocketjoe::dto
        rocketjoe::python_engine
        rocketjoe::flat_cache
        rocketjoe::router
        rocketjoe::control_block
        dl
)


install(TARGETS
        ${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

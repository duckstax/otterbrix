PROJECT(demo_network_service)

include_directories(${ROOT_PATH}/components/)

add_executable(
        ${PROJECT_NAME}
        main.cpp
)

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        log
        network_service
        ${CONAN_LIBS}
)

if (USE_TRACY)
    target_compile_definitions(${PROJECT_NAME} PRIVATE TRACY_ENABLE=1)
    #target_link_libraries(${PROJECT_NAME} PUBLIC tracy_client)
endif ()
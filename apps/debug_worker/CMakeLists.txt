set(CURRENT_TARGET debug_worker)

set(${CURRENT_TARGET}_headers

)

set(${CURRENT_TARGET}_sources
        main.cpp
)

find_package(Python3 COMPONENTS Development)
include_directories(${Python3_INCLUDE_DIRS})

add_executable(${CURRENT_TARGET}  ${${CURRENT_TARGET}_sources} ${${CURRENT_TARGET}_headers} )

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        goblin-engineer
        rocketjoe::http_server
        rocketjoe::python_sandbox
        rocketjoe::router
        dl
)

install(TARGETS
        ${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

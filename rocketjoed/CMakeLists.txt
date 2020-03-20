set(CURRENT_TARGET rocketjoed)

set(${CURRENT_TARGET}_headers
        configuration.hpp
        init_service.hpp
        )

set(${CURRENT_TARGET}_sources
        configuration.cpp
        init_service.cpp
        main.cpp
        )

find_package(Python3 COMPONENTS Development)
include_directories(${Python3_INCLUDE_DIRS})

add_executable(${CURRENT_TARGET} ${${CURRENT_TARGET}_sources} ${${CURRENT_TARGET}_headers})

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        goblin-engineer
        rocketjoe::python_sandbox
        rocketjoe::log
        dl
)

install(TARGETS
        ${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

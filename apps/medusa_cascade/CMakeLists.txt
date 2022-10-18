project(medusa_cascade)

include_directories(../../components)
include_directories(../../services)

set(${PROJECT_NAME}_headers
        dispatcher.cpp
        dispatcher.hpp
        forward.hpp
        )

set(${PROJECT_NAME}_sources
        dispatcher.cpp
        main.cpp
)


add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_sources} ${${PROJECT_NAME}_headers})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        ${CONAN_LIBS}
        rocketjoe::ql
        rocketjoe::log
        rocketjoe::storage
        network_service
        dl
)

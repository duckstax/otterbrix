project(medusa_cascade)

include_directories(../../components)
include_directories(../../services)

set(${PROJECT_NAME}_headers
        dispatcher.hpp
        database.hpp
)

set(${PROJECT_NAME}_sources
        database.cpp
        dispatcher.cpp
        main.cpp
)


add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_sources} ${${PROJECT_NAME}_headers})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        ${CONAN_LIBS}
        rocketjoe::log
        network_service
        dl
)

project(medusa_cascade)

include_directories(../../components)
include_directories(../../services)

set(${PROJECT_NAME}_headers

        protocol/base.hpp
        protocol/protocol.hpp
        protocol/request_select.hpp
        protocol/insert.hpp
        protocol/response_select.hpp

        collection.hpp
        database.hpp
        dispatcher.cpp
        dispatcher.hpp
        forward.hpp
        route.hpp
)

set(${PROJECT_NAME}_sources
        protocol/base.cpp
        protocol/request_select.cpp
        protocol/response_select.cpp
        protocol/insert.cpp

        collection.cpp
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

add_subdirectory(test)
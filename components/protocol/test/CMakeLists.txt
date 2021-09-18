project(test_protocol)

add_executable(${PROJECT_NAME} main.cpp ../request_select.cpp ../request_select.hpp ../insert.hpp ../insert.cpp)

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        rocketjoe::log
)

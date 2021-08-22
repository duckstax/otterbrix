set(CURRENT_TARGET test_protocol)

add_executable(${CURRENT_TARGET} main.cpp ../protocol/request_select.cpp ../protocol/request_select.hpp ../protocol/insert.hpp ../protocol/insert.cpp)

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        rocketjoe::log
)

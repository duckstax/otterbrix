set(CURRENT_TARGET timer_manager_base_test)

add_executable(${CURRENT_TARGET} main.cpp)

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        rocketjoe::timer_manager
)


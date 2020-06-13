set(CURRENT_TARGET test_base_process_pool)

add_executable(${CURRENT_TARGET} main.cpp)

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        rocketjoe::log
        rocketjoe::process_pool
)


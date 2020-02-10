set(CURRENT_TARGET process_pool_test_base)

add_executable(${CURRENT_TARGET} main.cpp)

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        rocketjoe::process_pool
)


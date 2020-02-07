set(CURRENT_TARGET ipc_message_queue_base_test)

add_executable(${CURRENT_TARGET} main.cpp)

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        rt
        rocketjoe::ipc_message_queue
)


set(CURRENT_TARGET test_buffer)

add_executable(${CURRENT_TARGET} main.cpp)

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        rocketjoe::buffer
        ${CONAN_LIBS}
)

add_test(${CURRENT_TARGET} ${CURRENT_TARGET})
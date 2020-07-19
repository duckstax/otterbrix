set(CURRENT_TARGET test_signature)

add_executable(${CURRENT_TARGET} main.cpp)

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        rocketjoe::python_sandbox
        ${CONAN_LIBS}
)

add_test( NAME ${CURRENT_TARGET} COMMAND ${CURRENT_TARGET})
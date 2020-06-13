set(CURRENT_TARGET test_zmq-hub)

add_executable(${CURRENT_TARGET} main.cpp)

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        rocketjoe::zmq-hub
        rocketjoe::log
        ${CONAN_LIBS}
)

#add_test(${CURRENT_TARGET} ${CURRENT_TARGET})
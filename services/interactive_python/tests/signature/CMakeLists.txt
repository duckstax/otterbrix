set(CURRENT_TARGET test_signature)

add_executable(${CURRENT_TARGET} main.cpp ../../jupyter/hmac.hpp ../../jupyter/hmac.cpp)

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        ${CONAN_LIBS}
)

add_test( NAME ${CURRENT_TARGET} COMMAND ${CURRENT_TARGET})
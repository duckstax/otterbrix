set(CURRENT_TARGET test_storage)

add_executable(${CURRENT_TARGET} main.cpp)

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        rocketjoe::storage
        ${CONAN_LIBS}
)

#add_test(${CURRENT_TARGET} ${CURRENT_TARGET})
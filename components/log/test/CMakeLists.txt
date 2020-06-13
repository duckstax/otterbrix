set(CURRENT_TARGET test_log)

add_executable(${CURRENT_TARGET} main.cpp)

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        rocketjoe::log
)


install(TARGETS
        ${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
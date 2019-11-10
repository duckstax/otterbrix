set(CURRENT_TARGET router_test)

add_executable(${CURRENT_TARGET} main.cpp)

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        rocketjoe::router
        goblin-engineer
        dl
)


install(TARGETS
        ${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
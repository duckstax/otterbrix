set(CURRENT_TARGET test_log)

add_executable(${CURRENT_TARGET} main.cpp)

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        otterbrix::log
        CONAN_PKG::spdlog
        CONAN_PKG::boost
        CONAN_PKG::fmt
)


install(TARGETS
        ${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
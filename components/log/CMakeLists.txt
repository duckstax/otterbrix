set(CURRENT_TARGET log)

list(APPEND CURRENT_${TARGET_HEADERS}
        log.hpp
)

list(APPEND CURRENT_${TARGET_HEADERS}
        log.cpp
)

add_library(ottergon_${CURRENT_TARGET}
            ${CURRENT_${TARGET_HEADERS}}
            ${CURRENT_${TARGET_HEADERS}}
)


add_library(ottergon::${CURRENT_TARGET} ALIAS ottergon_${CURRENT_TARGET})

set_property(TARGET ottergon_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        ottergon_${CURRENT_TARGET} PUBLIC
        CONAN_PKG::spdlog
        CONAN_PKG::boost
        CONAN_PKG::fmt
)

target_include_directories(
        ottergon_${CURRENT_TARGET}
        PUBLIC
        "../../../"
)

if(DEV_MODE)
    #add_subdirectory(test)
endif()

install(TARGETS
        ottergon_${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

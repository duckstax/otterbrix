set(CURRENT_TARGET timer_manager)

list(APPEND CURRENT_TARGET_HEADERS
        timer_manager.hpp

)

list(APPEND CURRENT_TARGET_SOURCES
        timer_manager.cpp
)


add_library(rocketjoe_${CURRENT_TARGET} STATIC
            ${CURRENT_TARGET_HEADERS}
            ${CURRENT_TARGET_SOURCES}
)


add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        rocketjoe_${CURRENT_TARGET} PRIVATE
        goblin-engineer
        rocketjoe::network
        ${Boost_LIBRARIES}
        ${CONAN_LIBS}
)

target_include_directories(
        rocketjoe_${CURRENT_TARGET}
        PUBLIC
        "${PROJECT_SOURCE_DIR}/services/header/rocketjoe/services/control_center"
)

install(TARGETS
        rocketjoe_${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

if (DEV_MODE)
    add_subdirectory(test)
endif()

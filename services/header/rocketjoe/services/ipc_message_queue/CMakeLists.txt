set(CURRENT_TARGET  ipc_message_queue)

list(APPEND CURRENT_TARGET_HEADERS
        detail/basic_message_queue.hpp
        detail/message_queue_service.hpp
        detail/message_queue.hpp
        ipc_message_queue.hpp

)

list(APPEND CURRENT_TARGET_SOURCES
        ipc_message_queue.cpp
)


add_library(rocketjoe_${CURRENT_TARGET} STATIC
            ${CURRENT_TARGET_HEADERS}
            ${CURRENT_TARGET_SOURCES}
)


add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        rocketjoe_${CURRENT_TARGET} PRIVATE
        rt
        goblin-engineer
        ${CONAN_LIBS}
)

target_include_directories(
        rocketjoe_${CURRENT_TARGET}
        PUBLIC
        "${PROJECT_SOURCE_DIR}/services/header/rocketjoe/services/ipc_message_queue"
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

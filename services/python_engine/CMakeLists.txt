set(CURRENT_TARGET python_engine)

list(APPEND CURRENT_TARGET_HEADERS
        header/rocketjoe/services/python_engine/python_engine.hpp
        header/rocketjoe/services/python_engine/device.hpp
        header/rocketjoe/services/python_engine/python_sandbox.hpp

)

list(APPEND CURRENT_TARGET_SOURCES
        python_engine.cpp
        python_sandbox.cpp
)

add_library(rocketjoe_${CURRENT_TARGET} STATIC
            ${CURRENT_TARGET_HEADERS}
            ${CURRENT_TARGET_SOURCES}
)

add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        rocketjoe_${CURRENT_TARGET}
        rocketjoe::http
        goblin-engineer
        ${Python3_LIBRARIES}
        ${CONAN_LIBRARIES}
)

target_include_directories(rocketjoe_${CURRENT_TARGET} PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/header" "${CMAKE_CURRENT_SOURCE_DIR}/../../")

install(TARGETS
        rocketjoe_${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

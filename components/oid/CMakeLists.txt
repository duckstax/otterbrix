set(CURRENT_TARGET oid)

list(APPEND CURRENT_${TARGET_HEADERS}
        hex.hpp
        oid.hpp
        oid_generator.hpp
        )

list(APPEND CURRENT_${TARGET_HEADERS}
        hex.cpp
        )

add_library(rocketjoe_${CURRENT_TARGET}
        ${CURRENT_${TARGET_HEADERS}}
        ${CURRENT_${TARGET_HEADERS}}
        )

add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        rocketjoe_${CURRENT_TARGET} PRIVATE
        ${Boost_LIBRARIES}
        ${CONAN_LIBS}
)

target_include_directories(
        rocketjoe_${CURRENT_TARGET}
        PUBLIC
)

if (DEV_MODE)
    add_subdirectory(test)
endif ()

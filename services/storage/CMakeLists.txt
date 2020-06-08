set(CURRENT_TARGET storage)

set(${CURRENT_TARGET}_HEADERS
        storage_engine.hpp
        hash_storage.hpp
        storage.hpp
)

set(${CURRENT_TARGET}_SOURCES
        storage_engine.cpp
        hash_storage.cpp
        storage.cpp
)

add_library(
        rocketjoe_${CURRENT_TARGET}
        STATIC
        ${${CURRENT_TARGET}_HEADERS}
        ${${CURRENT_TARGET}_SOURCES}
)


add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        rocketjoe_${CURRENT_TARGET} PRIVATE
        rocketjoe::log
        ${CONAN_LIBS}
)

target_include_directories(rocketjoe_${CURRENT_TARGET} PUBLIC "")

if (DEV_MODE)
    add_subdirectory(tests)
endif ()

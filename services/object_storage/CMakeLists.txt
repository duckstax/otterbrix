set(CURRENT_TARGET router)

find_package(libbsoncxx REQUIRED)
find_package(libmongocxx REQUIRED)


list(APPEND CURRENT_TARGET_HEADERS
        header/rocketjoe/services/object_storage/object_storage.hpp
        header/rocketjoe/services/object_storage/object_storage_w.hpp

)

list(APPEND CURRENT_TARGET_SOURCES
        object_storage.cpp
)


add_library(rocketjoe_${CURRENT_TARGET} STATIC
            ${CURRENT_TARGET_HEADERS}
            ${CURRENT_TARGET_SOURCES}
)


add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})
set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        rocketjoe_${CURRENT_TARGET}
        rocketjoe::api
        goblin-engineer

        ${LIBBSONCXX_LIBRARIES}
        ${LIBMONGOCXX_LIBRARIES}
        ${BSON_LIBRARIES}

)

target_include_directories(
        rocketjoe_${CURRENT_TARGET}
        PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/header"
        "${CMAKE_CURRENT_SOURCE_DIR}/../../"
        ${LIBMONGOCXX_INCLUDE_DIRS}
        ${LIBBSONCXX_INCLUDE_DIRS}
        ${BSON_INCLUDE_DIRS}
)

install(TARGETS
        rocketjoe_${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

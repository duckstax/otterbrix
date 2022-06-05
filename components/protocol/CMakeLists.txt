project(protocol CXX)

list(APPEND HEADER_${PROJECT_NAME}
        protocol.hpp
        )

list(APPEND SOURCE_${PROJECT_NAME}
        base.cpp
        create_database.cpp
        drop_database.cpp
        drop_collection.cpp
        create_collection.cpp
        insert_one.cpp
        insert_many.cpp
        delete_one.cpp
        delete_many.cpp
        update_one.cpp
        update_many.cpp
)


add_library(rocketjoe_${PROJECT_NAME}
            ${HEADER_${PROJECT_NAME}}
            ${SOURCE_${PROJECT_NAME}}
)


add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME} )

set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        rocketjoe_${PROJECT_NAME} PRIVATE
        ${CONAN_LIBS}
        rocketjoe::document
)

target_include_directories(
        rocketjoe_${PROJECT_NAME}
        PUBLIC
        protocol.hpp
)

if(DEV_MODE)
   # add_subdirectory(test)
endif()
project(protocol CXX)

list(APPEND HEADER_${PROJECT_NAME}

        )

list(APPEND SOURCE_${PROJECT_NAME}
        base.cpp
        database.cpp
        collection.cpp
        insert_one.cpp
        insert_many.cpp
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
        insert_one.hpp
        insert_many.hpp
)

if(DEV_MODE)
   # add_subdirectory(test)
endif()
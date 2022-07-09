project(ql CXX)

set( HEADER_${PROJECT_NAME}

)

set( SOURCE_${PROJECT_NAME}
        parser.cpp
        find.cpp
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

)

if(DEV_MODE)
   add_subdirectory(test)
endif()
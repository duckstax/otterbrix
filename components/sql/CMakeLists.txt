project(sql CXX)

set(SOURCE_${PROJECT_NAME}
    lexer/lexer.cpp
    lexer/token.cpp
    parser.cpp
    parser/common/parser_invalid.cpp
    parser/database/parser_database.cpp
)

add_library(rocketjoe_${PROJECT_NAME} ${SOURCE_${PROJECT_NAME}})
add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME})

set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
    rocketjoe_${PROJECT_NAME} PUBLIC
    rocketjoe::ql
)

target_include_directories(
    rocketjoe_${PROJECT_NAME}
    PUBLIC
)

if (DEV_MODE)
    add_subdirectory(test)
endif ()

project(convert_int_to_string)

add_executable(${PROJECT_NAME} main.cpp)

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        magic_enum::magic_enum
        actor-zeta::actor-zeta

)
project(protocol CXX)

list(APPEND HEADER_${PROJECT_NAME}
        base.cpp
        base.hpp
        CMakeLists.txt
        forward.hpp
        insert.cpp
        insert.hpp
        protocol.hpp
        request_select.cpp
        request_select.hpp
        response_select.cpp
        response_select.hpp
)

list(APPEND SOURCE_${PROJECT_NAME}
        base.cpp
        base.hpp
        CMakeLists.txt
        forward.hpp
        insert.cpp
        insert.hpp
        protocol.hpp
        request_select.cpp
        request_select.hpp
        response_select.cpp
        response_select.hpp
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
)

target_include_directories(
        rocketjoe_${PROJECT_NAME}
        PUBLIC
        "../../../"
)

if(DEV_MODE)
    add_subdirectory(test)
endif()

install(TARGETS
        rocketjoe_${PROJECT_NAME}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

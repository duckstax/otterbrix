set(CURRENT_TARGET protocol)

list(APPEND ${CURRENT_TARGET}_HEADERS
        protocol.hpp
        message_generated.h
)

list(APPEND ${CURRENT_TARGET}_SOURCES
        protocol.cpp
)

add_library(rocketjoe_${CURRENT_TARGET} STATIC
            ${${CURRENT_TARGET}_HEADERS}
            ${${CURRENT_TARGET}_SOURCES}
)



add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        rocketjoe_${CURRENT_TARGET} PRIVATE
)
#  /home/kotbegemot/.conan/data/flatc/1.12.0/_/_/package/44fcf6b9a7fb86b2586303e3db40189d3b511830/bin/flatc message.fbs --cpp  --cpp-std c++11 --scoped-enums --gen-object-api
#  /home/kotbegemot/.conan/data/flatc/1.12.0/_/_/package/44fcf6b9a7fb86b2586303e3db40189d3b511830/bin/flatc message.fbs --cpp --gen-object-api  --reflect-names --scoped-enums
#  /home/kotbegemot/.conan/data/protoc_installer/3.9.1/bincrafters/stable/package/c0c1ef10e3d0ded44179e28b669d6aed0277ca6a/bin/protoc --cpp_out=. message.proto
target_include_directories(rocketjoe_${CURRENT_TARGET} PUBLIC "")

if (DEV_MODE)
    add_subdirectory(tests)
endif()

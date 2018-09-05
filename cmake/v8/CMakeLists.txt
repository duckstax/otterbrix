# Downloads the declared version of v8 source code, builds it,
# and creates a .tgz ready for uploading to our third-party deps repo.
#
# Resulting files (<v8>.tgz amd <v8>.md5) placed in the output/
# subdir of the Jenkins workspace.

project(cb_dep_v8 NONE)

CMAKE_MINIMUM_REQUIRED (VERSION 2.8)

SET (DEP_NAME "v8")

get_filename_component(PARENT_DIR ${CMAKE_CURRENT_LIST_DIR} PATH)
SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PARENT_DIR})
include(CBDepsMacros)

include(CPack)
include(ExternalProject)

find_package(Git REQUIRED)


### Download, configure and build v8 ####################################

if (APPLE)
    SET(LIBRARY_FILES "<SOURCE_DIR>/out/x64.release/libv8.dylib")
else (APPLE)
    SET(LIBRARY_FILES "<SOURCE_DIR>/out/x64.release/lib.target/libv8.so")
endif (APPLE)

ExternalProject_Add(v8
        GIT_REPOSITORY https://github.com/trondn/v8.git
        GIT_TAG ${GIT_TAG}

        CONFIGURE_COMMAND make dependencies

        BUILD_IN_SOURCE 1
        BUILD_COMMAND make -j 8 library=shared x64

        INSTALL_DIR ${CMAKE_BINARY_DIR}/install

        INSTALL_COMMAND mkdir -p <INSTALL_DIR>/lib <INSTALL_DIR>/include
        COMMAND cp ${LIBRARY_FILES} <INSTALL_DIR>/lib
        COMMAND cp <SOURCE_DIR>/include/v8-debug.h <INSTALL_DIR>/include
        COMMAND cp <SOURCE_DIR>/include/v8-preparser.h <INSTALL_DIR>/include
        COMMAND cp <SOURCE_DIR>/include/v8-profiler.h <INSTALL_DIR>/include
        COMMAND cp <SOURCE_DIR>/include/v8-testing.h <INSTALL_DIR>/include
        COMMAND cp <SOURCE_DIR>/include/v8.h <INSTALL_DIR>/include
        COMMAND cp <SOURCE_DIR>/include/v8config.h <INSTALL_DIR>/include
        COMMAND cp <SOURCE_DIR>/include/v8stdint.h <INSTALL_DIR>/include
        COMMAND ${CMAKE_COMMAND} -E echo FILE "(COPY lib include DESTINATION \"\${CMAKE_INSTALL_PREFIX}\")" > <INSTALL_DIR>/CMakeLists.txt
        )

# OS X-only: Custom post-build step to set the shared library install name.
if (APPLE)
    ExternalProject_Add_Step(v8 install_name
            COMMAND install_name_tool -id @rpath/libv8.dylib out/x64.release/libv8.dylib
            DEPENDEES build
            DEPENDERS install
            WORKING_DIRECTORY <BINARY_DIR>
            )
endif(APPLE)

# Custom packaging step to actually create a tar.gz
set(OUTPUT_DIR "$ENV{WORKSPACE}/output/${DEP_NAME}/${DEP_VERSION}")
set(OUTPUT_FILE "${OUTPUT_DIR}/${DEP_NAME}-${PLATFORM}-${HOST_ARCH}-${DEP_VERSION}")

ExternalProject_Add_Step(v8 package
        COMMAND ${CMAKE_COMMAND} -E make_directory ${OUTPUT_DIR}
        COMMAND ${CMAKE_COMMAND} -E tar czf ${OUTPUT_FILE}.tgz .
        DEPENDEES install
        WORKING_DIRECTORY <INSTALL_DIR>
        )

_GENERATE_MD5_FILE(${OUTPUT_FILE}.tgz ${OUTPUT_FILE}.md5)

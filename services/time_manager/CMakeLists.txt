project(TimeManager)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -Wall")
set(SOURCE_FILES TimeManager.h)
set(SOURCE_FILES time_manager.cpp TimeManager.cpp)

add_executable(main ${SOURCE_FILES} ${HEADER_FILES})

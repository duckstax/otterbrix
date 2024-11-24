PROJECT(non_thread_scheduler LANGUAGES CXX)

add_library(${PROJECT_NAME} scheduler_test.cpp clock_test.cpp)

target_link_libraries(
        ${PROJECT_NAME} PUBLIC
        actor-zeta::actor-zeta
)


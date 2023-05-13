cmake_minimum_required(VERSION 3.0)

PROJECT(non_thread_scheduler LANGUAGES CXX)

add_library(${PROJECT_NAME} scheduler_test.cpp clock_test.cpp)

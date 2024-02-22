project(dataframe)

set(${PROJECT_NAME}_SOURCES
        column/column.cpp
        column/column_view.cpp
        column/copy.cpp
        column/make.cpp
        column/slice.cpp
        column/strings_column_view.cpp
        detail/cast.cpp
        detail/bits.cpp
        detail/bitmask.cpp
        detail/util.cpp
        dictionary/dictionary_column_view.cpp
        dictionary/make_dictionary.cpp
        lists/lists_column_view.cpp
        lists/make.cpp
        lists/slice.cpp
        scalar/scalar.cpp
        table/table.cpp
        table/slice.cpp
        bitmask.cpp
        traits.cpp
        types.cpp
)


add_library(otterbrix_${PROJECT_NAME}
        ${${PROJECT_NAME}_SOURCES}
        )


add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        CONAN_PKG::boost
        otterbrix::assert
        dl
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
    add_subdirectory(tests)
endif ()

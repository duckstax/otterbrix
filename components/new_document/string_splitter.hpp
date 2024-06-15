#pragma once

#include <iostream>

class string_split_iterator {
public:
    using iterator_category = std::input_iterator_tag;
    using value_type = std::string_view;
    using difference_type = std::ptrdiff_t;
    using pointer = const value_type*;
    using reference = const value_type&;

    string_split_iterator(std::string_view str, char delim, bool end = false) noexcept;

    reference operator*() const noexcept;

    pointer operator->() const noexcept;

    string_split_iterator& operator++() noexcept;

    string_split_iterator operator++(int) noexcept;

    friend bool operator==(const string_split_iterator& a, const string_split_iterator& b) noexcept;

    friend bool operator!=(const string_split_iterator& a, const string_split_iterator& b) noexcept;

private:
    std::string_view str_;
    char delim_;
    bool end_ = false;
    bool next_end_ = false;
    value_type current_;
};

class string_splitter {
public:
    string_splitter(std::string_view str, char delim) noexcept;

    string_split_iterator begin() const noexcept;

    string_split_iterator end() const noexcept;

private:
    std::string_view str_;
    char delim_;
};

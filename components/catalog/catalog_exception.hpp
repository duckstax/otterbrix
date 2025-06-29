#pragma once

#include <exception>
#include <string>

namespace components::catalog {
    class catalog_exception : public std::exception {
    protected:
        std::string message_;

    public:
        explicit catalog_exception(const std::string& message)
            : message_(message) {}

        [[nodiscard]] const char* what() const noexcept override { return message_.c_str(); }
    };

    class no_such_table_exception : public catalog_exception {
    public:
        explicit no_such_table_exception(const std::string& message)
            : catalog_exception(message) {}
    };

    class no_such_namespace_exception : public catalog_exception {
    public:
        explicit no_such_namespace_exception(const std::string& message)
            : catalog_exception(message) {}
    };

    class already_exists_exception : public catalog_exception {
    public:
        explicit already_exists_exception(const std::string& message)
            : catalog_exception(message) {}
    };

    class commit_failed_exception : public catalog_exception {
    public:
        explicit commit_failed_exception(const std::string& message)
            : catalog_exception(message) {}
    };

    class schema_exception : public catalog_exception {
    public:
        explicit schema_exception(const std::string& message)
            : catalog_exception(message) {}
    };

    class not_supported_exception : public catalog_exception {
    public:
        explicit not_supported_exception(const std::string& message)
            : catalog_exception(message) {}
    };
} // namespace components::catalog

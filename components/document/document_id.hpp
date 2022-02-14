#pragma once
#include <string>

namespace components::document {

    class document_id_t {
    public:
        explicit document_id_t(std::string id);
        explicit document_id_t(const std::string_view &id);
        document_id_t(const document_id_t &id) = default;
        document_id_t(document_id_t &&id) noexcept;

        document_id_t &operator=(std::string id);
        document_id_t &operator=(const std::string_view &id);
        document_id_t &operator=(const document_id_t &id);
        document_id_t &operator=(document_id_t &&id);

        static document_id_t generate();
        static document_id_t null_id();

        bool operator==(const document_id_t &other) const;
        bool operator!=(const document_id_t &other) const;
        bool operator<(const document_id_t &other) const;

        [[nodiscard]] std::string to_string() const;
        [[nodiscard]] std::string_view to_string_view() const;

        [[nodiscard]] bool is_null() const;

    private:
        std::string value_;
        bool null_ {false};

        document_id_t();
    };

}
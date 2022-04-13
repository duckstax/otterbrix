#pragma once

#include <oid/oid_generator.hpp>
#include <memory>

namespace components::document {

    class document_id_t : public oid_t<12> {
        using super = oid_t<12>;
        using generator_t = oid_generator_t<4, 5, 3>;
        using generator_ptr = std::shared_ptr<generator_t>;

    public:
        explicit document_id_t(const std::string &id);
        explicit document_id_t(std::string_view id);
        document_id_t(const document_id_t &id) = default;
        document_id_t(document_id_t &&id) noexcept;

        document_id_t &operator=(const document_id_t &id);
        document_id_t &operator=(document_id_t &&id) noexcept;

        static document_id_t generate();
        static document_id_t null_id();

        [[nodiscard]] document_id_t next() const;
        [[nodiscard]] bool is_null() const;

    private:
        generator_ptr generator_;

        explicit document_id_t(const super &id, generator_ptr generator);
    };

}
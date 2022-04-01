#pragma once
#include <string>
#include <vector>
#include <components/document/document.hpp>
#include <components/protocol/base.hpp>

namespace services::disk {

    class result_read_databases {
    public:
        using result_t = std::vector<database_name_t>;

        result_read_databases() = default;
        explicit result_read_databases(result_t&& databases);
        [[nodiscard]] const result_t& databases() const;

    private:
        result_t databases_;
    };


    class result_read_collections {
    public:
        using result_t = std::vector<collection_name_t>;

        result_read_collections() = default;
        explicit result_read_collections(result_t&& collections);
        [[nodiscard]] const result_t& collections() const;

    private:
        result_t collections_;
    };


    class result_read_documents {
    public:
        using result_t = std::vector<components::document::document_ptr>;

        result_read_documents() = default;
        explicit result_read_documents(result_t&& documents);
        [[nodiscard]] const result_t& documents() const;

    private:
        result_t documents_;
    };

} //namespace services::disk
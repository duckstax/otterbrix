#pragma once

#include <vector>
#include <components/protocol/base.hpp>
#include <components/document/document.hpp>

namespace services::disk {

    struct result_collection_t {
        collection_name_t name;
        std::vector<components::document::document_ptr> documents;
    };

    struct result_database_t {
        database_name_t name;
        std::vector<result_collection_t> collections;

        void set_collection(const std::vector<collection_name_t> &names);
    };

    class result_load_t {
        using result_t = std::vector<result_database_t>;

    public:
        result_load_t(const std::vector<database_name_t> &databases);
        const result_t& operator*() const;
        result_t& operator*();

    private:
        result_t databases_;
    };

} // namespace services::disk

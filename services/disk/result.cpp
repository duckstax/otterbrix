#include "result.hpp"

namespace services::disk {

    result_read_databases::result_read_databases(result_read_databases::result_t&& databases)
        :databases_(std::move(databases)) {
    }

    const result_read_databases::result_t& result_read_databases::databases() const {
        return databases_;
    }


    result_read_collections::result_read_collections(result_read_collections::result_t&& collections)
        : collections_(std::move(collections)) {
    }

    const result_read_collections::result_t& result_read_collections::collections() const {
        return collections_;
    }


    result_read_documents::result_read_documents(result_read_documents::result_t&& documents)
        : documents_(std::move(documents)) {
    }

    const result_read_documents::result_t& result_read_documents::documents() const {
        return documents_;
    }

} //namespace services::disk
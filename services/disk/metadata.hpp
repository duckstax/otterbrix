#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <boost/filesystem.hpp>
#include <components/protocol/base.hpp>

namespace services::disk {

    class metadata_t {

        using path_t = boost::filesystem::path;
        using collections_t = std::vector<collection_name_t>;
        using databases_t = std::vector<database_name_t>;
        using data_t = std::unordered_map<database_name_t, collections_t>;
        using metadata_ptr = std::unique_ptr<metadata_t>;

    public:
        static metadata_ptr open(const path_t &file_name);

        databases_t databases() const;
        const collections_t &collections(const database_name_t &database) const;

        bool is_exists_database(const database_name_t &database) const;
        bool append_database(const database_name_t &database, bool is_flush = true);
        bool remove_database(const database_name_t &database, bool is_flush = true);

        bool is_exists_collection(const database_name_t &database, const collection_name_t &collection) const;
        bool append_collection(const database_name_t &database, const collection_name_t &collection, bool is_flush = true);
        bool remove_collection(const database_name_t &database, const collection_name_t &collection, bool is_flush = true);

        metadata_t() = delete;
        metadata_t(const metadata_t &) = delete;
        metadata_t(metadata_t &&) = delete;
        metadata_t &operator=(metadata_t const&) = delete;
        metadata_t &operator=(metadata_t &&) = delete;
    private:
        data_t data_;
        path_t file_name_;

        explicit metadata_t(const path_t &file_name);
        void flush_();
    };

} //namespace services::disk
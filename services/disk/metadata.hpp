#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>

namespace services::disk {

    class metadata_t {
    public:
        using database_name_t = std::string;
        using collection_name_t = std::string;
        using collections_t = std::vector<collection_name_t>;
        using databases_t = std::vector<collection_name_t>;
        using data_t = std::unordered_map<database_name_t, collections_t>;
        using packdata_t = std::string;
        using metadata_ptr = std::unique_ptr<metadata_t>;

        packdata_t pack() const;
        static metadata_ptr extract(const packdata_t &package);

        databases_t databases() const;
        const collections_t &collections(const database_name_t &database) const;

        bool is_exists_database(const database_name_t &database) const;
        void append_database(const database_name_t &database);
        void remove_database(const database_name_t &database);

        bool is_exists_collection(const database_name_t &database, const collection_name_t &collection) const;
        void append_collection(const database_name_t &database, const collection_name_t &collection);
        void remove_collection(const database_name_t &database, const collection_name_t &collection);

        metadata_t() = delete;
        metadata_t(const metadata_t &) = delete;
        metadata_t(metadata_t &&) = delete;
        metadata_t &operator=(metadata_t const&) = delete;
        metadata_t &operator=(metadata_t &&) = delete;
        explicit metadata_t(const packdata_t &package);

    private:
        data_t data_;
    };

} //namespace services::disk
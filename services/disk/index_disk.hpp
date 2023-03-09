#pragma once

#include <components/document/document_id.hpp>
#include <components/document/wrapper_value.hpp>
#include <filesystem>

namespace rocksdb {
    class DB;
    class Comparator;
} // namespace rocksdb

namespace services::disk {

    class index_disk {
        using document_id_t = components::document::document_id_t;
        using wrapper_value_t = document::wrapper_value_t;
        using path_t = std::filesystem::path;
        using result = std::vector<document_id_t>;

    public:
        enum class compare {
            str,
            numeric
        };

        index_disk(const path_t& path, compare compare_type);
        ~index_disk();

        void insert(const wrapper_value_t& key, const document_id_t& value);
        void remove(wrapper_value_t key);
        result find(const wrapper_value_t& value) const;
        result lower_bound(const wrapper_value_t& value) const;
        result upper_bound(const wrapper_value_t& value) const;

    private:
        std::unique_ptr<rocksdb::DB> db_;
        std::unique_ptr<rocksdb::Comparator> comparator_;
    };

} // namespace services::disk

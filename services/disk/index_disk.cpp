#include "index_disk.hpp"
#include <rocksdb/db.h>

namespace services::disk {

    class str_comparator final : public rocksdb::Comparator {
    public:
        str_comparator() = default;

    private:
        int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const final {
            return a.compare(b);
        }

        const char* Name() const final {
            return "str_comparator";
        }

        void FindShortestSeparator(std::string* start, const rocksdb::Slice& limit) const final {
            auto size = limit.size();
            if (start->size() < size) {
                start->resize(size);
            }
        }

        void FindShortSuccessor(std::string*) const final {
        }
    };

    std::unique_ptr<rocksdb::Comparator> make_comparator(index_disk::compare compare_type) {
        switch (compare_type) {
            case index_disk::compare::str:
                return std::make_unique<str_comparator>();
            case index_disk::compare::numeric:
                return std::make_unique<str_comparator>(); //todo
        }
        return std::make_unique<str_comparator>();
    }

    index_disk::index_disk(const path_t& path, compare compare_type)
        : db_(nullptr)
        , comparator_(make_comparator(compare_type)) {
        rocksdb::Options options;
        options.OptimizeLevelStyleCompaction();
        options.create_if_missing = true;
        options.comparator = comparator_.get();
        rocksdb::DB* db;
        auto status = rocksdb::DB::Open(options, path.string(), &db);
        if (status.ok()) {
            db_.reset(db);
        } else {
            throw std::runtime_error("db open failed");
        }
    }

    index_disk::~index_disk() = default;

    void index_disk::insert(const wrapper_value_t& key, const document_id_t& value) {
        db_->Put(rocksdb::WriteOptions(), key->as_string(), value.to_string());
    }

    void index_disk::remove(wrapper_value_t key) {
        db_->Delete(rocksdb::WriteOptions(), key->as_string());
    }

    index_disk::result index_disk::find(const wrapper_value_t& value) const {
        index_disk::result res;
        std::string sbuf;
        auto status = db_->Get(rocksdb::ReadOptions(), value->as_string(), &sbuf);
        if (!status.IsNotFound()) {
            res.emplace_back(sbuf);
        }
        return res;
    }

    index_disk::result index_disk::lower_bound(const wrapper_value_t& value) const {
        index_disk::result res;
        rocksdb::Iterator* it = db_->NewIterator(rocksdb::ReadOptions());
        for (it->SeekForPrev(value->as_string()); it->Valid(); it->Prev()) {
            if (it->key().ToString() != value->as_string()) {
                res.emplace_back(it->value().ToString());
            }
        }
        delete it;
        std::reverse(res.begin(), res.end());
        return res;
    }

    index_disk::result index_disk::upper_bound(const wrapper_value_t& value) const {
        index_disk::result res;
        rocksdb::Iterator* it = db_->NewIterator(rocksdb::ReadOptions());
        for (it->Seek(value->as_string()); it->Valid(); it->Next()) {
            if (it->key().ToString() != value->as_string()) {
                res.emplace_back(it->value().ToString());
            }
        }
        delete it;
        return res;
    }

} // namespace services::disk

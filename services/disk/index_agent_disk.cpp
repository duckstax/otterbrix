#include "index_agent_disk.hpp"
#include <rocksdb/db.h>
#include "manager_disk.hpp"
#include "result.hpp"
#include "route.hpp"

namespace services::disk {

    index_agent_disk_t::index_agent_disk_t(base_manager_disk_t* manager,
                                           const path_t& path_db,
                                           const collection_name_t& collection_name,
                                           const index_name_t& index_name,
                                           log_t& log)
        : actor_zeta::basic_async_actor(manager, index_name)
        , log_(log.clone())
        , db_(nullptr) {
        trace(log_, "index_agent_disk::create {}", index_name);
        rocksdb::Options options;
        //options.IncreaseParallelism();
        options.OptimizeLevelStyleCompaction();
        options.create_if_missing = true;
        rocksdb::DB *db;
        auto path = path_db / "indexes" / collection_name / index_name;
        auto status = rocksdb::DB::Open(options, path.string(), &db);
        if (status.ok()) {
            db_.reset(db);
        } else {
            throw std::runtime_error("db open failed");
        }
    }

    index_agent_disk_t::~index_agent_disk_t() {
        trace(log_, "delete index_agent_disk_t");
    }

} //namespace services::disk
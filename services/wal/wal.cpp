#include "wal.hpp"
#include <unistd.h>
#include <utility>

#include <crc32c/crc32c.h>
#include "dto.hpp"
#include "route.hpp"

namespace services::wal {

    constexpr static auto wal_name = ".wal";

    bool file_exist_(const boost::filesystem::path &path) {
        boost::filesystem::file_status s = boost::filesystem::file_status{};
        return boost::filesystem::status_known(s)
                   ? boost::filesystem::exists(s)
                   : boost::filesystem::exists(path);
    }

    wal_replicate_t::wal_replicate_t(manager_wal_replicate_t*manager, log_t& log, boost::filesystem::path path)
        : actor_zeta::basic_async_actor(manager, "wal")
        , log_(log.clone())
        , path_(std::move(path)) {
        add_handler(handler_id(route::create_database), &wal_replicate_t::create_database);
        add_handler(handler_id(route::drop_database), &wal_replicate_t::drop_database);
        add_handler(handler_id(route::create_collection), &wal_replicate_t::create_collection);
        add_handler(handler_id(route::drop_collection), &wal_replicate_t::drop_collection);
        add_handler(handler_id(route::insert_one), &wal_replicate_t::insert_one);
        add_handler(handler_id(route::insert_many), &wal_replicate_t::insert_many);
        if (!file_exist_(path_)) {
            boost::filesystem::create_directory(path_);
        }
        file_ = std::make_unique<components::file::file_t>(path_ / wal_name);
    }

    void wal_replicate_t::send_success(session_id_t& session, address_t& sender) {
        if (sender) {
            actor_zeta::send(sender, address(), handler_id(route::success), session, services::wal::id_t(id_));
        }
    }

    wal_replicate_t::~wal_replicate_t() {
    }

    size_tt read_size_impl(buffer_t& input, int index_start) {
        size_tt size_tmp = 0;
        size_tmp = 0xff00 & size_tt(input[index_start] << 8);
        size_tmp |= 0x00ff & size_tt(input[index_start + 1]);
        return size_tmp;
    }

    static size_tt read_size_impl(const char* input, int index_start) {
        size_tt size_tmp = 0;
        size_tmp = 0xff00 & (size_tt(input[index_start] << 8));
        size_tmp |= 0x00ff & (size_tt(input[index_start + 1]));
        return size_tmp;
    }

    size_tt wal_replicate_t::read_size(size_t start_index) {
        auto size_read = sizeof(size_tt);
        auto buffer = file_->read(size_read, off64_t(start_index));
        auto size_blob = read_size_impl(buffer.data(), 0);
        return size_blob;
    }

    buffer_t wal_replicate_t::read(size_t start_index, size_t finish_index) {
        auto size_read = finish_index - start_index;
        buffer_t buffer;
        file_->read(buffer, size_read, off64_t(start_index));
        return buffer;
    }

    void wal_replicate_t::create_database(session_id_t& session, address_t& sender, components::protocol::create_database_t& data) {
        trace(log_, "wal_replicate_t::create_database {}", data.database_);
        buffer_.clear();
        last_crc32_ = pack(buffer_, last_crc32_, id_, data);
        write_();
        send_success(session, sender);
    }

    void wal_replicate_t::drop_database(session_id_t& session, address_t& sender, components::protocol::drop_database_t& data) {
        trace(log_, "wal_replicate_t::drop_database {}", data.database_);
        buffer_.clear();
        last_crc32_ = pack(buffer_, last_crc32_, id_, data);
        write_();
        send_success(session, sender);
    }

    void wal_replicate_t::create_collection(session_id_t& session, address_t& sender, components::protocol::create_collection_t& data) {
        trace(log_, "wal_replicate_t::create_collection {}::{}", data.database_, data.collection_);
        buffer_.clear();
        last_crc32_ = pack(buffer_, last_crc32_, id_, data);
        write_();
        send_success(session, sender);
    }

    void wal_replicate_t::drop_collection(session_id_t& session, address_t& sender, components::protocol::drop_collection_t& data) {
        trace(log_, "wal_replicate_t::drop_collection {}::{}", data.database_, data.collection_);
        buffer_.clear();
        last_crc32_ = pack(buffer_, last_crc32_, id_, data);
        write_();
        send_success(session, sender);
    }

    void wal_replicate_t::insert_one(session_id_t& session, address_t& sender, insert_one_t& data) {
        trace(log_, "wal_replicate_t::insert_one {}::{}", data.database_, data.collection_);
        buffer_.clear();
        last_crc32_ = pack(buffer_, last_crc32_, id_, data);
        write_();
        send_success(session, sender);
    }

    void wal_replicate_t::insert_many(session_id_t& session, address_t& sender, insert_many_t& data) {
        trace(log_, "wal_replicate_t::insert_many {}::{}", data.database_, data.collection_);
        buffer_.clear();
        last_crc32_ = pack(buffer_, last_crc32_, id_, data);
        write_();
        send_success(session, sender);
    }

    void wal_replicate_t::write_() {
        file_->append(buffer_.data(), buffer_.size());
        writed_ += buffer_.size();
        next_id(id_);
    }

} //namespace services::wal

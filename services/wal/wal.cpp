#include "wal.hpp"
#include <unistd.h>
#include <utility>

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
        add_handler(handler_id(route::load), &wal_replicate_t::load);
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
        file_->seek_eof();
        init_id();
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

    size_tt wal_replicate_t::read_size(size_t start_index) const {
        auto size_read = sizeof(size_tt);
        auto buffer = file_->read(size_read, off64_t(start_index));
        auto size_blob = read_size_impl(buffer.data(), 0);
        return size_blob;
    }

    buffer_t wal_replicate_t::read(size_t start_index, size_t finish_index) const {
        auto size_read = finish_index - start_index;
        buffer_t buffer;
        file_->read(buffer, size_read, off64_t(start_index));
        return buffer;
    }

    void wal_replicate_t::load(session_id_t& session, address_t& sender, services::wal::id_t wal_id) {
        trace(log_, "wal_replicate_t::load, id: {}", wal_id);
        std::size_t start_index = 0;
        next_id(wal_id);
        if (find_start_record(wal_id, start_index)) {
            debug(log_, "wal_id: {}, start_index: {}", wal_id, start_index);
            //todo: load list commands
        }
        //todo: send list commands
        actor_zeta::send(sender, address(), handler_id(route::load_finish), session);
    }

    void wal_replicate_t::create_database(session_id_t& session, address_t& sender, components::protocol::create_database_t& data) {
        trace(log_, "wal_replicate_t::create_database {}", data.database_);
        write_data_(data);
        send_success(session, sender);
    }

    void wal_replicate_t::drop_database(session_id_t& session, address_t& sender, components::protocol::drop_database_t& data) {
        trace(log_, "wal_replicate_t::drop_database {}", data.database_);
        write_data_(data);
        send_success(session, sender);
    }

    void wal_replicate_t::create_collection(session_id_t& session, address_t& sender, components::protocol::create_collection_t& data) {
        trace(log_, "wal_replicate_t::create_collection {}::{}", data.database_, data.collection_);
        write_data_(data);
        send_success(session, sender);
    }

    void wal_replicate_t::drop_collection(session_id_t& session, address_t& sender, components::protocol::drop_collection_t& data) {
        trace(log_, "wal_replicate_t::drop_collection {}::{}", data.database_, data.collection_);
        write_data_(data);
        send_success(session, sender);
    }

    void wal_replicate_t::insert_one(session_id_t& session, address_t& sender, insert_one_t& data) {
        trace(log_, "wal_replicate_t::insert_one {}::{}", data.database_, data.collection_);
        write_data_(data);
        send_success(session, sender);
    }

    void wal_replicate_t::insert_many(session_id_t& session, address_t& sender, insert_many_t& data) {
        trace(log_, "wal_replicate_t::insert_many {}::{}", data.database_, data.collection_);
        write_data_(data);
        send_success(session, sender);
    }

    template<class T>
    void wal_replicate_t::write_data_(T &data) {
        next_id(id_);
        buffer_.clear();
        last_crc32_ = pack(buffer_, last_crc32_, id_, data);
        file_->append(buffer_.data(), buffer_.size());
    }

    void wal_replicate_t::init_id() {
        std::size_t start_index = 0;
        auto id = read_id(start_index);
        while (id > 0) {
            id_ = id;
            start_index += read_size(start_index) + sizeof(size_tt) + sizeof(crc32_t);
            id = read_id(start_index);
        }
    }

    bool wal_replicate_t::find_start_record(services::wal::id_t wal_id, std::size_t &start_index) const {
        start_index = 0;
        auto first_id = read_id(start_index);
        if (first_id > 0) {
            for (auto n = first_id; n < wal_id; ++n) {
                auto size = read_size(start_index);
                if (size > 0) {
                    start_index += size + sizeof(size_tt) + sizeof(crc32_t);
                } else {
                    return false;
                }
            }
            return wal_id == read_id(start_index);
        }
        return false;
    }

    services::wal::id_t wal_replicate_t::read_id(std::size_t start_index) const {
        auto size = read_size(start_index);
        if (size > 0) {
            auto start = start_index + sizeof(size_tt);
            auto finish = start + size;
            auto output = read(start, finish);
            return unpack_wal_id(output);
        }
        return 0;
    }

} //namespace services::wal

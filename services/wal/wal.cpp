#include "wal.hpp"
#include <crc32c/crc32c.h>
#include <fcntl.h>

#include <utility>

#include <chrono>
#include <ctime>
#include <iostream>
#include <unistd.h>

std::string getTimeStr() {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);

    std::stringstream ss;
    ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H:%M:%S");
    return ss.str();
}

void append_crc32(buffer_t& storage, crc32_t crc32) {
    storage.push_back(buffer_element_t(crc32 >> 24));
    storage.push_back((buffer_element_t(crc32 >> 16)));
    storage.push_back((buffer_element_t(crc32 >> 8)));
    storage.push_back((buffer_element_t(crc32)));
}

void append_size(buffer_t& storage, size_tt size) {
    storage.push_back((buffer_element_t((size >> 8))));
    storage.push_back(buffer_element_t(size));
}

void append_payload(buffer_t& storage, char* ptr, size_t size) {
    storage.insert(std::end(storage), ptr, ptr + size);
}

crc32_t read_crc32(buffer_t& input,int index_start) {
    crc32_t crc32_tmp = 0;
    crc32_tmp = (uint32_t) input[index_start] << 24;
    crc32_tmp |= (uint32_t) input[index_start+1] << 16;
    crc32_tmp |= (uint32_t) input[index_start+2] << 8;
    crc32_tmp |= (uint32_t) input[index_start+3];
    return crc32_tmp;
}

buffer_t read_payload(buffer_t& input,int index_start,int index_stop){
    buffer_t buffer(input.begin()+index_start,input.begin()+index_stop);
    return buffer;
}

size_tt read_size(buffer_t& input,int index_start) {
    size_tt size_tmp = 0;
    size_tmp = (size_tt) input[ index_start] << 8;
    size_tmp |= (size_tt) input[ index_start +1 ];
    return size_tmp;
}

constexpr static auto wal_name = ".wal";

void wal_t::add_event(Type type, buffer_t& data) {
    buffer_.clear();
    entry_t entry(last_crc32_,type,log_number_,data);
    last_crc32_ = pack(buffer_, entry);

    std::vector<iovec> iodata;
    iodata.emplace_back(iovec{buffer_.data(), buffer_.size()});
    int size_write = ::pwritev(fd_, iodata.data(), iodata.size(), writed_);
    writed_ += size_write;
    ++log_number_;
}
wal_t::wal_t(goblin_engineer::supervisor_t* manager, log_t& log, std::filesystem::path path)
    : goblin_engineer::abstract_service(manager, "wal")
    , log_(log.clone())
    , path_(std::move(path)) {
    auto not_existed = not file_exist_(path_);
    if (not_existed) {
        std::filesystem::create_directory(path_);
        open_file(path);
        header_write_(path_);
    }
    open_file(path);
    auto not_check = not header_check_(path_);
    if (not_check) {
        // abort();
    }
}

bool wal_t::file_exist_(std::filesystem::path path) {
    std::filesystem::file_status s = std::filesystem::file_status{};
    std::cout << path;
    if (std::filesystem::status_known(s) ? std::filesystem::exists(s) : std::filesystem::exists(path)) {
        log_.trace("exists");
        return true;
    } else {
        log_.trace("does not exist");
        return false;
    }
}

bool wal_t::header_check_(std::filesystem::path path) {
}

void wal_t::header_write_(std::filesystem::path path) {
    /*
    buffer_.clear();
    buffer_t empty;
    pack(Type::init, buffer_, log_number_, empty);
    std::vector<iovec> iodata;
    iodata.emplace_back(iovec{buffer_.data(), buffer_.size()});
    int size_write = ::pwritev(fd_, iodata.data(), iodata.size(), writed_);
    writed_ += size_write;
    ++log_number_;
     */
}

void wal_t::open_file(std::filesystem::path) {
    auto file_name = getTimeStr();
    file_name.append(wal_name);
    auto file_path = path_ / file_name;
    fd_ = ::open(file_path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0777);
}

wal_t::~wal_t() {

}

crc32_t pack(buffer_t& storage,entry_t&entry) {
    msgpack::sbuffer input_1;
    msgpack::pack(input_1, entry);
    auto last_crc32_ = crc32c::Crc32c(input_1.data(), input_1.size());
    append_size(storage, input_1.size());
    append_payload(storage, input_1.data(), input_1.size());
    append_crc32(storage, last_crc32_);
    return last_crc32_;
}

void unpack(buffer_t& storage, wal_entry_t&entry) {
    entry.size_ = read_size(storage,0);
    auto start = sizeof(size_tt);
    auto finish = sizeof(size_tt)+entry.size_-1;
    auto buffer = read_payload(storage, start,finish);
    msgpack::unpacked msg;
    msgpack::unpack(msg, reinterpret_cast<char*>(buffer.data()), buffer.size());
    const auto& o = msg.get();
    entry.entry_ = o.as<entry_t>();
    entry.crc32_ = read_crc32(storage,sizeof(size_tt)+1+entry.size_+1);

}

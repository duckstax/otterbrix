#include "wal.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <ctime>

#include <utility>
#include <chrono>

#include <crc32c/crc32c.h>

std::string get_time_to_string() {
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

crc32_t read_crc32(buffer_t& input, int index_start) {
    crc32_t crc32_tmp = 0;
    crc32_tmp = (uint32_t) input[index_start] << 24;
    crc32_tmp |= (uint32_t) input[index_start + 1] << 16;
    crc32_tmp |= (uint32_t) input[index_start + 2] << 8;
    crc32_tmp |= (uint32_t) input[index_start + 3];
    return crc32_tmp;
}

buffer_t read_payload(buffer_t& input, int index_start, int index_stop) {
    buffer_t buffer(input.begin() + index_start, input.begin() + index_stop);
    return buffer;
}

size_tt read_size_impl(buffer_t& input, int index_start) {
    size_tt size_tmp = 0;
    size_tmp = (size_tt) input[index_start] << 8;
    size_tmp |= (size_tt) input[index_start + 1];
    return size_tmp;
}

size_tt read_size_impl(char* input, int index_start) {
    size_tt size_tmp = 0;
    size_tmp = (size_tt) input[index_start] << 8;
    size_tmp |= (size_tt) input[index_start + 1];
    return size_tmp;
}

constexpr static auto wal_name = ".wal";

void wal_t::add_event(Type type, buffer_t& data) {
    buffer_.clear();
    entry_t entry(last_crc32_, type, log_number_, data);
    last_crc32_ = pack(buffer_, entry);

    std::vector<iovec> iodata;
    iodata.emplace_back(iovec{buffer_.data(), buffer_.size()});
    int size_write = ::pwritev(fd_, iodata.data(), iodata.size(), writed_);
    writed_ += size_write;
    ++log_number_;
}

auto open_file(boost::filesystem::path path) {
    auto file_name = get_time_to_string();
    file_name.append(wal_name);
    auto file_path = path / file_name;
    auto fd_ = ::open(file_path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0777);
    return fd_;
}

wal_t::wal_t(goblin_engineer::supervisor_t* manager, log_t& log, boost::filesystem::path path)
    : goblin_engineer::abstract_service(manager, "wal")
    , log_(log.clone())
    , path_(std::move(path)) {
    auto not_existed = not file_exist_(path_);
    if (not_existed) {
        boost::filesystem::create_directory(path_);
        fd_ = open_file(path);
    }

    fd_ = open_file(path);
}

bool wal_t::file_exist_(boost::filesystem::path path) {
    boost::filesystem::file_status s = boost::filesystem::file_status{};
    log_.trace(path.c_str());
    if (boost::filesystem::status_known(s) ? boost::filesystem::exists(s) : boost::filesystem::exists(path)) {
        log_.trace("exists");
        return true;
    } else {
        log_.trace("does not exist");
        return false;
    }
}

wal_t::~wal_t() {
    ::close(fd_);
}

size_tt wal_t::read_size(size_t start_index) {
    auto size_read = sizeof(size_tt);
    auto buffer = std::make_unique<char[]>(size_read);
    ::pread(fd_, buffer.get(), size_read, start_index);
    auto size_blob = read_size_impl(buffer.get(), 0);
    return size_blob;
}

buffer_t wal_t::read(size_t start_index, size_t finish_index) {
    auto size_read = finish_index - start_index;
    buffer_t buffer;
    buffer.resize(size_read);
    ::pread(fd_, buffer.data(), size_read, start_index);
    return buffer;
}

crc32_t pack(buffer_t& storage, entry_t& entry) {
    msgpack::sbuffer input;
    msgpack::pack(input, entry);
    auto last_crc32_ = crc32c::Crc32c(input.data(), input.size());
    append_size(storage, input.size());
    append_payload(storage, input.data(), input.size());
    append_crc32(storage, last_crc32_);
    return last_crc32_;
}

void unpack(buffer_t& storage, wal_entry_t& entry) {
    entry.size_ = read_size_impl(storage, 0);
    auto start = sizeof(size_tt);
    auto finish = sizeof(size_tt) + entry.size_;
    auto buffer = read_payload(storage, start, finish);
    msgpack::unpacked msg;
    msgpack::unpack(msg, buffer.data(), buffer.size());
    const auto& o = msg.get();
    entry.entry_ = o.as<entry_t>();
    auto crc32_index = sizeof(size_tt) + entry.size_;
    entry.crc32_ = read_crc32(storage, crc32_index);
}

void unpack_v2(buffer_t& storage, wal_entry_t& entry) {
    auto start = 0;
    auto finish = entry.size_;
    auto buffer = read_payload(storage, start, finish);
    msgpack::unpacked msg;
    msgpack::unpack(msg, buffer.data(), buffer.size());
    const auto& o = msg.get();
    entry.entry_ = o.as<entry_t>();
    entry.crc32_ = read_crc32(storage, sizeof(size_tt) + entry.size_);
}

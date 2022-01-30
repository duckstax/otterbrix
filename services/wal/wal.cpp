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

void append_crc32_init(buffer_t & storage) {
    storage[0]=buffer_element_t((0 >> 24));
    storage[1]=(buffer_element_t(0 >> 16));
    storage[2]=(buffer_element_t(0 >> 8));
    storage[3]=(buffer_element_t(0));
}

void set_crc32(buffer_t & storage, crc32_t crc32) {
    storage[0] = buffer_element_t(crc32 >> 24);
    storage[1] = (buffer_element_t(crc32 >> 16));
    storage[2] = (buffer_element_t(crc32 >> 8));
    storage[3] = (buffer_element_t(crc32));
}

void append_size(buffer_t & storage, size_tt size) {
    storage[4]=(buffer_element_t(size));
    storage[5]=(buffer_element_t((size >> 8) ));
}

void append_type(buffer_t & storage, Type type) {
    storage[6]=(buffer_element_t(type));
}

void append_log_number(buffer_t & storage, log_number_t number) {
    storage[7]=(buffer_element_t(number >> 8 * 0));
    storage[8]=(buffer_element_t(number >> 8 * 1));
    storage[9]=(buffer_element_t(number >> 8 * 2));
    storage[10]=(buffer_element_t(number >> 8 * 3));
    storage[11]=(buffer_element_t(number >> 8 * 4));
    storage[12]=(buffer_element_t(number >> 8 * 5));
    storage[13]=(buffer_element_t(number >> 8 * 6));
    storage[14]=(buffer_element_t(number >> 8 * 7));
}

void append_payload(buffer_t & storage, buffer_t payload) {
    storage.insert(end(storage), begin(payload), end(payload));
}
/*
void pack(entry_t&entry ,buffer_t & out) {
    const auto size = sizeof(size_tt)+ entry.payload_.size() + sizeof(crc32_t);
    append_crc32_init(out);
    append_size(out, size);
    append_type(out, type);
    append_log_number(out, number);
    append_payload(out, data);
    char* ptr = reinterpret_cast<char*>(out.data());
    ptr += sizeof(crc32_t);
    auto size_crc32 = out.size() - sizeof(crc32_t);
    crc32_t crc32 = crc32c::Crc32c(ptr, size_crc32);
    set_crc32(out, crc32);
}
*/
crc32_t read_crc32(buffer_t & input) {
    crc32_t crc32_tmp=0;
    crc32_tmp = (uint32_t) input[0] << 24;
    crc32_tmp |= (uint32_t) input[1] << 16;
    crc32_tmp |= (uint32_t) input[2] << 8;
    crc32_tmp |= (uint32_t) input[3];
    return crc32_tmp;
}

size_tt read_size(buffer_t & input) {
    size_tt size_tmp=0;
    size_tmp = (size_tt) input[4] << 8;
    size_tmp |= (size_tt) input[5] ;
    return size_tmp;
}

Type read_type(buffer_t & input) {
    return static_cast<Type>(input[6]);
}

log_number_t read_log_number(buffer_t & input) {
    log_number_t tmp=0;
    tmp = (uint64_t) input[7] << 8 * 7;
    tmp |= (uint64_t) input[8] << 8 * 6;
    tmp |= (uint64_t) input[9] << 8 * 5;
    tmp |= (uint64_t) input[10] << 8 * 4;
    tmp |= (uint64_t) input[11] << 8 * 3;
    tmp |= (uint64_t) input[12] << 8 * 2;
    tmp |= (uint64_t) input[13] << 8;
    tmp |= (uint64_t) input[14] ;
    return tmp;
}
/*
void unpack(buffer_t & input, wal_entry_t& e) {
    e.crc32_ = read_crc32(input);
    auto size = read_size(input);
    e.type_ = read_type(input);
    e.log_number_ = read_log_number(input);
    e.payload_ = buffer_t(input.begin() + heer_size, input.begin() + size);
}
*/
constexpr static auto wal_name = ".wal";

void wal_t::add_event(Type type, buffer_t & data) {
    /*
    buffer_.clear();
    const auto size = heer_size + data.size();
    buffer_.resize(size);
    pack(type, data, log_number_, buffer_);
    std::vector<iovec> iodata;
    iodata.emplace_back(iovec{buffer_.data(), buffer_.size()});
    int size_write = ::pwritev(fd_, iodata.data(), iodata.size(), writed_);
    writed_ += size_write;
    ++log_number_;
     */
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


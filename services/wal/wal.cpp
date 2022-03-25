#include "wal.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <ctime>

#include <utility>
#include <chrono>

#include <crc32c/crc32c.h>
#include "dto.hpp"

std::string get_time_to_string() {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);

    std::stringstream ss;
    ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H:%M:%S");
    return ss.str();
}

constexpr static auto wal_name = ".wal";

auto open_file(boost::filesystem::path path) {
    auto file_name = get_time_to_string();
    file_name.append(wal_name);
    auto file_path = path / file_name;
    auto fd_ = ::open(file_path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0777);
    return fd_;
}

wal_replicate_t::wal_replicate_t(goblin_engineer::supervisor_t* manager,log_t& log, boost::filesystem::path path)
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

bool wal_replicate_t::file_exist_(boost::filesystem::path path) {
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

wal_replicate_t::~wal_replicate_t() {
    ::close(fd_);
}

size_tt read_size_impl(buffer_t& input, int index_start) {
    size_tt size_tmp = 0;
    size_tmp = 0xff00 & size_tt(input[index_start] << 8);
    size_tmp |= 0x00ff & size_tt(input[index_start + 1]);
    return size_tmp;
}

static size_tt read_size_impl(char* input, int index_start) {
    size_tt size_tmp = 0;
    size_tmp = 0xff00 & (size_tt(input[index_start] << 8));
    size_tmp |= 0x00ff & (size_tt(input[index_start + 1]));
    return size_tmp;
}

size_tt wal_replicate_t::read_size(size_t start_index) {
    auto size_read = sizeof(size_tt);
    auto buffer = std::make_unique<char[]>(size_read);
    ::pread(fd_, buffer.get(), size_read, start_index);
    auto size_blob = read_size_impl(buffer.get(), 0);
    return size_blob;
}

buffer_t wal_replicate_t::read(size_t start_index, size_t finish_index) {
    auto size_read = finish_index - start_index;
    buffer_t buffer;
    buffer.resize(size_read);
    ::pread(fd_, buffer.data(), size_read, start_index);
    return buffer;
}

void wal_replicate_t::insert_many(insert_many_t& data) {
    buffer_.clear();
    last_crc32_ = pack(buffer_,last_crc32_,log_number_,data) ;
    write_();
}

void wal_replicate_t::write_() {
    std::vector<iovec> iodata;
    iodata.emplace_back(iovec{buffer_.data(), buffer_.size()});
    int size_write = ::pwritev(fd_, iodata.data(), iodata.size(), writed_);
    writed_ += size_write;
    ++log_number_;
}

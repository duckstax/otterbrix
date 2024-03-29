#include "base_file_system.hpp"

#include "utils.hpp"

#include <cstdint>
#include <cstdio>

#ifndef _WIN32
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef __MVS__
#define _XOPEN_SOURCE_EXTENDED 1
#include <sys/resource.h>
// enjoy - https://reviews.llvm.org/D92110
#define PATH_MAX _XOPEN_PATH_MAX
#endif

#else
#include <string>
#include <sysinfoapi.h>

#ifdef __MINGW32__
// need to manually define this for mingw
extern "C" WINBASEAPI BOOL WINAPI GetPhysicallyInstalledSystemMemory(PULONGLONG);
#endif

#undef FILE_CREATE // woo mingw
#endif

namespace core::file {

    base_file_system_t::~base_file_system_t() {
    }

    bool path_matched(const std::string& path, const std::string& sub_path) {
        return path.rfind(sub_path, 0) == 0;
    }

    #ifndef _WIN32

    std::string base_file_system_t::enviroment_variable(const std::string& name) {
        const char* env = getenv(name.c_str());
        if (!env) {
            return std::string();
        }
        return env;
    }

    bool base_file_system_t::is_path_absolute(const std::string& path) {
        auto separator = path_separator();
        return path_matched(path, separator);
    }

    std::string base_file_system_t::path_separator() {
        return "/";
    }

    bool base_file_system_t::set_working_directory(const std::string& path) {
        if (chdir(path.c_str()) != 0) {
            return false;
        }
        return true;
    }

    uint64_t base_file_system_t::available_memory() {
        errno = 0;

    #ifdef __MVS__
        struct rlimit limit;
        int rlim_rc = getrlimit(RLIMIT_AS, &limit);
        uint64_t max_memory = std::min<uint64_t>(limit.rlim_max, UINTPTR_MAX);
    #else
        uint64_t max_memory = std::min<uint64_t>((uint64_t)sysconf(_SC_PHYS_PAGES) * (uint64_t)sysconf(_SC_PAGESIZE), UINTPTR_MAX);
    #endif
        if (errno != 0) {
            return INVALID_INDEX;
        }
        return max_memory;
    }

    std::string base_file_system_t::working_directory() {
        auto buffer = std::make_unique<char[]>(PATH_MAX);
        char* ret = getcwd(buffer.get(), PATH_MAX);
        if (!ret) {
            return std::string();
        }
        return std::string(buffer.get());
    }

    std::string base_file_system_t::normalize_path_absolute(const std::string& path) {
        assert(is_path_absolute(path));
        return path;
    }

    #else

    std::string base_file_system_t::enviroment_variable(const std::string& env) {
        // first convert the environment variable name to the correct encoding
        auto env_w = string_utils::UTF8_to_Unicode(env.c_str());
        // use _wgetenv to get the value
        auto res_w = _wgetenv(env_w.c_str());
        if (!res_w) {
            // no environment variable of this name found
            return std::string();
        }
        return string_utils::Unicode_to_UTF8(res_w);
    }

    static bool StartsWithSingleBackslash(const std::string& path) {
        if (path.size() < 2) {
            return false;
        }
        if (path[0] != '/' && path[0] != '\\') {
            return false;
        }
        if (path[1] == '/' || path[1] == '\\') {
            return false;
        }
        return true;
    }

    bool base_file_system_t::is_path_absolute(const std::string& path) {
        // 1) A single backslash or forward-slash
        if (StartsWithSingleBackslash(path)) {
            return true;
        }
        // 2) special "long paths" on windows
        if (path_matched(path, "\\\\?\\")) {
            return true;
        }
        // 3) a network path
        if (path_matched(path, "\\\\")) {
            return true;
        }
        // 4) A disk designator with a backslash (e.g., C:\ or C:/)
        auto path_aux = path;
        path_aux.erase(0, 1);
        if (path_matched(path_aux, ":\\") || path_matched(path_aux, ":/")) {
            return true;
        }
        return false;
    }

    std::string base_file_system_t::normalize_path_absolute(const std::string& path) {
        assert(is_path_absolute(path));
        auto result = string_utils::lower(base_file_system_t::convert_separators(path));
        if (StartsWithSingleBackslash(result)) {
            // Path starts with a single backslash or forward slash
            // prepend drive letter
            return working_directory().substr(0, 2) + result;
        }
        return result;
    }

    std::string base_file_system_t::path_separator() {
        return "\\";
    }

    bool base_file_system_t::set_working_directory(const std::string& path) {
        auto unicode_path = string_utils::UTF8_to_Unicode(path.c_str());
        if (!SetCurrentDirectoryW(unicode_path.c_str())) {
            return false;;
        }
        return true;
    }

    uint64_t base_file_system_t::available_memory() {
        ULONGLONG available_memory_kb;
        if (GetPhysicallyInstalledSystemMemory(&available_memory_kb)) {
            return std::min<uint64_t>(available_memory_kb * 1000, UINTPTR_MAX);
        }
        // fallback: try GlobalMemoryStatusEx
        MEMORYSTATUSEX mem_state;
        mem_state.dwLength = sizeof(MEMORYSTATUSEX);

        if (GlobalMemoryStatusEx(&mem_state)) {
            return std::min<uint64_t>(mem_state.ullTotalPhys, UINTPTR_MAX);
        }
        return INVALID_INDEX;
    }

    std::string base_file_system_t::working_directory() {
        uint64_t count = GetCurrentDirectoryW(0, nullptr);
        if (count == 0) {
            return std::string();
        }
        auto buffer = std::make_unique<wchar_t[]>(count);
        uint64_t ret = GetCurrentDirectoryW(count, buffer.get());
        if (count != ret + 1) {
            return std::string();
        }
        return string_utils::Unicode_to_UTF8(buffer.get());
    }

    #endif

    std::string base_file_system_t::join_path(const std::string& a, const std::string& b) {
        // FIXME: sanitize paths
        return a.empty() ? b : a + path_separator() + b;
    }

    std::string base_file_system_t::convert_separators(const std::string& path) {
        auto separator_str = path_separator();
        char separator = separator_str[0];
        if (separator == '/') {
            // on unix-based systems we only accept / as a separator
            return path;
        }
        // on windows-based systems we accept both
        return string_utils::replace(path, "/", separator_str);
    }

    std::string base_file_system_t::extract_name(const std::string& path) {
        if (path.empty()) {
            return std::string();
        }
        auto normalized_path = convert_separators(path);
        auto sep = path_separator();
        auto splits = string_utils::split(normalized_path, sep);
        assert(!splits.empty());
        return splits.back();
    }

    std::string base_file_system_t::extract_base_name(const std::string& path) {
        if (path.empty()) {
            return std::string();
        }
        auto vec = string_utils::split(extract_name(path), ".");
        assert(!vec.empty());
        return vec[0];
    }

    std::string base_file_system_t::home_directory() {
        // read the home_directory setting first, if it is set
        if (!home_directory_.empty()) {
            return home_directory_;
        }
        // fallback to the default home directories for the specified system
    #ifdef PLATFORM_WINDOWS
        return base_file_system_t::enviroment_variable("USERPROFILE");
    #else
        return base_file_system_t::enviroment_variable("HOME");
    #endif
    }

    std::string base_file_system_t::expand_path(const std::string& path) {
        if (path.empty()) {
            return path;
        }
        if (path[0] == '~') {
            return home_directory() + path.substr(1);
        }
        return path;
    }

    // LCOV_EXCL_START

    bool base_file_system_t::has_glob(const std::string& str) {
        for (uint64_t i = 0; i < str.size(); i++) {
            switch (str[i]) {
            case '*':
            case '?':
            case '[':
                return true;
            default:
                break;
            }
        }
        return false;
    }
    

    void base_file_system_t::reset(file_handle_t& handle) {
        handle.seek(0);
    }

    file_handle_t::file_handle_t(base_file_system_t& file_system, std::string path_p) : file_system_(file_system), path_(std::move(path_p)) {
    }

    file_handle_t::~file_handle_t() {
    }

    int64_t file_handle_t::read(void* buffer, uint64_t nr_bytes) {
        return file_system_.read(*this, buffer, nr_bytes);
    }

    int64_t file_handle_t::write(void* buffer, uint64_t nr_bytes) {
        return file_system_.write(*this, buffer, nr_bytes);
    }

    void file_handle_t::read(void* buffer, uint64_t nr_bytes, uint64_t location) {
        file_system_.read(*this, buffer, nr_bytes, location);
    }

    void file_handle_t::write(void* buffer, uint64_t nr_bytes, uint64_t location) {
        file_system_.write(*this, buffer, nr_bytes, location);
    }

    bool file_handle_t::seek(uint64_t location) {
        return file_system_.seek(*this, location);
    }

    void file_handle_t::reset() {
        file_system_.reset(*this);
    }

    uint64_t file_handle_t::seek_position() {
        return file_system_.seek_position(*this);
    }

    bool file_handle_t::can_seek() {
        return file_system_.can_seek();
    }

    bool file_handle_t::is_pipe() {
        return file_system_.is_pipe(path_);
    }

    std::string file_handle_t::read_line() {
        std::string result;
        char buffer[1];
        while (true) {
            uint64_t tuples_read = read(buffer, 1);
            if (tuples_read == 0 || buffer[0] == '\n') {
                return result;
            }
            if (buffer[0] != '\r') {
                result += buffer[0];
            }
        }
    }

    uint64_t file_handle_t::file_size() {
        return file_system_.file_size(*this);
    }

    void file_handle_t::sync() {
        file_system_.file_sync(*this);
    }

    bool file_handle_t::truncate(int64_t new_size) {
        return file_system_.truncate(*this, new_size);
    }

    file_type_t file_handle_t::type() {
        return file_system_.file_type(*this);
    }

} // namespace core::file

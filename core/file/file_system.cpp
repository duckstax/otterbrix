#include "file_system.hpp"

#include "utils.hpp"
#include <limits>
#include <algorithm>

#include <cstdint>
#include <cstdio>
#include <sys/stat.h>

#ifndef _WIN32
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#else

#include <io.h>
#include <string>

#ifdef __MINGW32__
// need to manually define this for mingw
extern "C" WINBASEAPI BOOL WINAPI GetPhysicallyInstalledSystemMemory(PULONGLONG);
#endif

#undef FILE_CREATE // woo mingw
#endif

// includes for giving a better error message on lock conflicts
#if defined(__linux__) || defined(__APPLE__)
#include <pwd.h>
#endif

#if defined(__linux__)
#include <libgen.h>
// See e.g.:
// https://opensource.apple.com/source/CarbonHeaders/CarbonHeaders-18.1/TargetConditionals.h.auto.html
#elif defined(__APPLE__)
#include <TargetConditionals.h>                             // NOLINT
#if not(defined(TARGET_OS_IPHONE) && TARGET_OS_IPHONE == 1) // NOLINT
#include <libproc.h>                                        // NOLINT
#endif                                                      // NOLINT
#elif defined(_WIN32)
#include <restartmanager.h>
#endif

namespace core::file {

    #ifndef _WIN32
    bool file_system_t::file_exists(const std::string& filename) {
        if (!filename.empty()) {
            if (access(filename.c_str(), 0) == 0) {
                struct stat status;
                stat(filename.c_str(), &status);
                if (S_ISREG(status.st_mode)) {
                    return true;
                }
            }
        }
        // if any condition fails
        return false;
    }

    bool file_system_t::is_pipe(const std::string& filename) {
        if (!filename.empty()) {
            if (access(filename.c_str(), 0) == 0) {
                struct stat status;
                stat(filename.c_str(), &status);
                if (S_ISFIFO(status.st_mode)) {
                    return true;
                }
            }
        }
        // if any condition fails
        return false;
    }

    #else
    bool file_system_t::file_exists(const std::string& filename) {
        auto unicode_path = string_utils::UTF8_to_Unicode(filename.c_str());
        const wchar_t* wpath = unicode_path.c_str();
        if (_waccess(wpath, 0) == 0) {
            struct _stati64 status;
            _wstati64(wpath, &status);
            if (status.st_mode & S_IFREG) {
                return true;
            }
        }
        return false;
    }
    bool file_system_t::is_pipe(const std::string& filename) {
        auto unicode_path = string_utils::UTF8_to_Unicode(filename.c_str());
        const wchar_t* wpath = unicode_path.c_str();
        if (_waccess(wpath, 0) == 0) {
            struct _stati64 status;
            _wstati64(wpath, &status);
            if (status.st_mode & _S_IFCHR) {
                return true;
            }
        }
        return false;
    }
    #endif

    #ifndef _WIN32
    // somehow sometimes this is missing
    #ifndef O_CLOEXEC
    #define O_CLOEXEC 0
    #endif

    // Solaris
    #ifndef O_DIRECT
    #define O_DIRECT 0
    #endif

    struct unix_file_handle_t : public file_handle_t {
    public:
        unix_file_handle_t(base_file_system_t& file_system, std::string path, int fd) : file_handle_t(file_system, std::move(path)), fd(fd) {
        }
        ~unix_file_handle_t() override {
            unix_file_handle_t::close();
        }

        int fd;

    public:
        void close() override {
            if (fd != -1) {
                ::close(fd);
                fd = -1;
            }
        };
    };

    static file_type_t file_type_internal(int fd) { // LCOV_EXCL_START
        struct stat s;
        if (fstat(fd, &s) == -1) {
            return file_type_t::FILE_TYPE_INVALID;
        }
        switch (s.st_mode & S_IFMT) {
        case S_IFBLK:
            return file_type_t::FILE_TYPE_BLOCKDEV;
        case S_IFCHR:
            return file_type_t::FILE_TYPE_CHARDEV;
        case S_IFIFO:
            return file_type_t::FILE_TYPE_FIFO;
        case S_IFDIR:
            return file_type_t::FILE_TYPE_DIR;
        case S_IFLNK:
            return file_type_t::FILE_TYPE_LINK;
        case S_IFREG:
            return file_type_t::FILE_TYPE_REGULAR;
        case S_IFSOCK:
            return file_type_t::FILE_TYPE_SOCKET;
        default:
            return file_type_t::FILE_TYPE_INVALID;
        }
    } // LCOV_EXCL_STOP

    std::unique_ptr<file_handle_t> file_system_t::open_file(const std::string& path_p, uint8_t flags, file_lock_type lock_type) {
        auto path = base_file_system_t::expand_path(path_p);

        int open_flags = 0;
        int rc;
        bool open_read = flags & file_flags::FILE_FLAGS_READ;
        bool open_write = flags & file_flags::FILE_FLAGS_WRITE;
        if (open_read && open_write) {
            open_flags = O_RDWR;
        } else if (open_read) {
            open_flags = O_RDONLY;
        } else if (open_write) {
            open_flags = O_WRONLY;
        } else {
            return nullptr;
        }
        if (open_write) {
            // need read or write
            assert(flags & file_flags::FILE_FLAGS_WRITE);
            open_flags |= O_CLOEXEC;
            if (flags & file_flags::FILE_FLAGS_FILE_CREATE) {
                open_flags |= O_CREAT;
            } else if (flags & file_flags::FILE_FLAGS_FILE_CREATE_NEW) {
                open_flags |= O_CREAT | O_TRUNC;
            }
            if (flags & file_flags::FILE_FLAGS_APPEND) {
                open_flags |= O_APPEND;
            }
        }
        if (flags & file_flags::FILE_FLAGS_DIRECT_IO) {
    #if defined(__sun) && defined(__SVR4)
            throw std::logic_error("DIRECT_IO not supported on Solaris");
    #endif
    #if defined(__DARWIN__) || defined(__APPLE__) || defined(__OpenBSD__)
            // OSX does not have O_DIRECT, instead we need to use fcntl afterwards to support direct IO
            open_flags |= O_SYNC;
    #else
            open_flags |= O_DIRECT | O_SYNC;
    #endif
        }
        int fd = open(path.c_str(), open_flags, 0666);
        if (fd == -1) {
            return nullptr;
        }
        if (lock_type != file_lock_type::NO_LOCK) {
            // set lock on file
            // but only if it is not an input/output stream
            auto file_type_t = file_type_internal(fd);
            if (file_type_t != file_type_t::FILE_TYPE_FIFO && file_type_t != file_type_t::FILE_TYPE_SOCKET) {
                struct flock fl;
                memset(&fl, 0, sizeof fl);
                fl.l_type = lock_type == file_lock_type::READ_LOCK ? F_RDLCK : F_WRLCK;
                fl.l_whence = SEEK_SET;
                fl.l_start = 0;
                fl.l_len = 0;
                rc = fcntl(fd, F_SETLK, &fl);
                // Retain the original error.
                int retained_errno = errno;
                if (rc == -1) {
                    return nullptr;
                }
            }
        }
        return std::make_unique<unix_file_handle_t>(*this, path, fd);
    }

    bool file_system_t::set_file_pointer(file_handle_t& handle, uint64_t location) {
        int fd = handle.cast<unix_file_handle_t>().fd;
        off_t offset = lseek(fd, location, SEEK_SET);
        if (offset == (off_t)-1) {
            return false;
        }
        return true;
    }

    uint64_t file_system_t::file_pointer(file_handle_t& handle) {
        int fd = handle.cast<unix_file_handle_t>().fd;
        off_t position = lseek(fd, 0, SEEK_CUR);
        if (position == (off_t)-1) {
            return INVALID_INDEX;
        }
        return position;
    }

    bool file_system_t::read(file_handle_t& handle, void* buffer, int64_t nr_bytes, uint64_t location) {
        int fd = handle.cast<unix_file_handle_t>().fd;
        auto read_buffer = reinterpret_cast<char*>(buffer);
        while (nr_bytes > 0) {
            int64_t bytes_read = pread(fd, read_buffer, nr_bytes, location);
            if (bytes_read == -1 || bytes_read == 0) {
                return false;
            }
            read_buffer += bytes_read;
            nr_bytes -= bytes_read;
        }
        return true;
    }

    int64_t file_system_t::read(file_handle_t& handle, void* buffer, int64_t nr_bytes) {
        int fd = handle.cast<unix_file_handle_t>().fd;
        int64_t bytes_read = ::read(fd, buffer, nr_bytes);
        return bytes_read;
    }

    bool file_system_t::write(file_handle_t& handle, void* buffer, int64_t nr_bytes, uint64_t location) {
        int fd = handle.cast<unix_file_handle_t>().fd;
        auto write_buffer = reinterpret_cast<char*>(buffer);
        while (nr_bytes > 0) {
            int64_t bytes_written = pwrite(fd, write_buffer, nr_bytes, location);
            if (bytes_written <= 0) {
                return false;
            }
            write_buffer += bytes_written;
            nr_bytes -= bytes_written;
        }
        return true;
    }

    int64_t file_system_t::write(file_handle_t& handle, void* buffer, int64_t nr_bytes) {
        int fd = handle.cast<unix_file_handle_t>().fd;
        int64_t bytes_written = 0;
        while (nr_bytes > 0) {
            auto bytes_to_write = std::min<uint64_t>(uint64_t(std::numeric_limits<int32_t>::max()), uint64_t(nr_bytes));
            int64_t current_bytes_written = ::write(fd, buffer, bytes_to_write);
            if (current_bytes_written <= 0) {
                return current_bytes_written;
            }
            bytes_written += current_bytes_written;
            buffer = buffer + current_bytes_written;
            nr_bytes -= current_bytes_written;
        }
        return bytes_written;
    }

    int64_t file_system_t::file_size(file_handle_t& handle) {
        int fd = handle.cast<unix_file_handle_t>().fd;
        struct stat s;
        if (fstat(fd, &s) == -1) {
            return -1;
        }
        return s.st_size;
    }

    time_t file_system_t::last_modified_time(file_handle_t& handle) {
        int fd = handle.cast<unix_file_handle_t>().fd;
        struct stat s;
        if (fstat(fd, &s) == -1) {
            return -1;
        }
        return s.st_mtime;
    }

    file_type_t file_system_t::file_type(file_handle_t& handle) {
        int fd = handle.cast<unix_file_handle_t>().fd;
        return file_type_internal(fd);
    }

    bool file_system_t::truncate(file_handle_t &handle, int64_t new_size) {
        int fd = handle.cast<unix_file_handle_t>().fd;
        if (ftruncate(fd, new_size) != 0) {
            return false;
        }
        return true;
    }

    bool file_system_t::directory_exists(const std::string& directory) {
        if (!directory.empty()) {
            if (access(directory.c_str(), 0) == 0) {
                struct stat status;
                stat(directory.c_str(), &status);
                if (status.st_mode & S_IFDIR) {
                    return true;
                }
            }
        }
        // if any condition fails
        return false;
    }

    bool file_system_t::create_directory(const std::string& directory) {
        struct stat st;

        if (stat(directory.c_str(), &st) != 0) {
            /* Directory does not exist. EEXIST for race condition */
            if (mkdir(directory.c_str(), 0755) != 0 && errno != EEXIST) {
                return false;
            }
        } else if (!S_ISDIR(st.st_mode)) {
            return false;
        }
        return true;
    }

    int RemoveDirectoryRecursive(const char* path) {
        DIR *d = opendir(path);
        uint64_t path_len = (uint64_t)strlen(path);
        int r = -1;

        if (d) {
            struct dirent *p;
            r = 0;
            while (!r && (p = readdir(d))) {
                int r2 = -1;
                char *buf;
                uint64_t len;
                /* Skip the names "." and ".." as we don't want to recurse on them. */
                if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, "..")) {
                    continue;
                }
                len = path_len + (uint64_t)strlen(p->d_name) + 2;
                buf = new (std::nothrow) char[len];
                if (buf) {
                    struct stat statbuf;
                    snprintf(buf, len, "%s/%s", path, p->d_name);
                    if (!stat(buf, &statbuf)) {
                        if (S_ISDIR(statbuf.st_mode)) {
                            r2 = RemoveDirectoryRecursive(buf);
                        } else {
                            r2 = unlink(buf);
                        }
                    }
                    delete[] buf;
                }
                r = r2;
            }
            ::closedir(d);
        }
        if (!r) {
            r = rmdir(path);
        }
        return r;
    }

    bool file_system_t::remove_directory(const std::string& directory) {
        return RemoveDirectoryRecursive(directory.c_str()) != -1;
    }

    bool file_system_t::remove_file(const std::string& filename) {
        if (std::remove(filename.c_str()) != 0) {
            return false;
        }
        return true;
    }

    bool file_system_t::list_files(const std::string& directory, const std::function<void(const std::string&, bool)>& callback) {
        if (!directory_exists(directory)) {
            return false;
        }
        DIR *dir = opendir(directory.c_str());
        if (!dir) {
            return false;
        }
        struct dirent* ent;
        // loop over all files in the directory
        while ((ent = readdir(dir)) != nullptr) {
            std::string name = std::string(ent->d_name);
            // skip . .. and empty files
            if (name.empty() || name == "." || name == "..") {
                continue;
            }
            // now stat the file to figure out if it is a regular file or directory
            std::string full_path = join_path(directory, name);
            if (access(full_path.c_str(), 0) != 0) {
                continue;
            }
            struct stat status;
            stat(full_path.c_str(), &status);
            if (!(status.st_mode & S_IFREG) && !(status.st_mode & S_IFDIR)) {
                // not a file or directory: skip
                continue;
            }
            // invoke callback
            callback(name, status.st_mode & S_IFDIR);
        }
        ::closedir(dir);
        return true;
    }

    bool file_system_t::file_sync(file_handle_t& handle) {
        int fd = handle.cast<unix_file_handle_t>().fd;
        if (fsync(fd) != 0) {
            return false;
        }
        return true;
    }

    bool file_system_t::move_files(const std::string& source, const std::string& target) {
        // FIXME: rename does not guarantee atomicity or overwriting target file if it exists
        if (rename(source.c_str(), target.c_str()) != 0) {
            return false;
        }
        return true;
    }

    std::string file_system_t::last_error_as_string() {
        return std::string();
    }

    #else

    constexpr char PIPE_PREFIX[] = "\\\\.\\pipe\\";

    // Returns the last Win32 error, in std::string format. Returns an empty std::string if there is no error.
    std::string file_system_t::last_error_as_string() {
        // Get the error message, if any.
        DWORD errorMessageID = GetLastError();
        if (errorMessageID == 0)
            return std::string(); // No error message has been recorded

        LPSTR messageBuffer = nullptr;
        uint64_t size =
            FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                        NULL, errorMessageID, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR)&messageBuffer, 0, NULL);

        std::string message(messageBuffer, size);

        // Free the buffer.
        LocalFree(messageBuffer);

        return message;
    }

    struct windows_file_handle_t : public file_handle_t {
    public:
        windows_file_handle_t(base_file_system_t& file_system, std::string path, HANDLE fd)
            : file_handle_t(file_system, path), position_(0), fd_(fd) {
        }
        ~windows_file_handle_t() override {
            close();
        }

        uint64_t position_;
        HANDLE fd_;

    public:
        void close() override {
            if (!fd_) {
                return;
            }
            CloseHandle(fd_);
            fd_ = nullptr;
        };
    };

    std::unique_ptr<file_handle_t> file_system_t::open_file(const std::string& path_p, uint8_t flags, file_lock_type) {
        auto path = base_file_system_t::expand_path(path_p);

        DWORD desired_access;
        DWORD share_mode;
        DWORD creation_disposition = OPEN_EXISTING;
        DWORD flags_and_attributes = FILE_ATTRIBUTE_NORMAL;
        bool open_read = flags & file_flags::FILE_FLAGS_READ;
        bool open_write = flags & file_flags::FILE_FLAGS_WRITE;
        if (open_read && open_write) {
            desired_access = GENERIC_READ | GENERIC_WRITE;
            share_mode = 0;
        } else if (open_read) {
            desired_access = GENERIC_READ;
            share_mode = FILE_SHARE_READ;
        } else if (open_write) {
            desired_access = GENERIC_WRITE;
            share_mode = 0;
        } else {
            return nullptr;
        }
        if (open_write) {
            if (flags & file_flags::FILE_FLAGS_FILE_CREATE) {
                creation_disposition = OPEN_ALWAYS;
            } else if (flags & file_flags::FILE_FLAGS_FILE_CREATE_NEW) {
                creation_disposition = CREATE_ALWAYS;
            }
        }
        if (flags & file_flags::FILE_FLAGS_DIRECT_IO) {
            flags_and_attributes |= FILE_FLAG_NO_BUFFERING;
        }
        auto unicode_path = string_utils::UTF8_to_Unicode(path.c_str());
        HANDLE hFile = CreateFileW(unicode_path.c_str(), desired_access, share_mode, NULL, creation_disposition,
                                flags_and_attributes, NULL);
        if (hFile == INVALID_HANDLE_VALUE) {
            auto error = file_system_t::last_error_as_string();

            return nullptr;
        }
        auto handle = std::make_unique<windows_file_handle_t>(*this, path.c_str(), hFile);
        if (flags & file_flags::FILE_FLAGS_APPEND) {
            set_file_pointer(*handle, file_size(*handle));
        }
        return std::move(handle);
    }

    bool file_system_t::set_file_pointer(file_handle_t& handle, uint64_t location) {
        auto &whandle = handle.cast<windows_file_handle_t>();
        whandle.position_ = location;
        LARGE_INTEGER wlocation;
        wlocation.QuadPart = location;
        SetFilePointerEx(whandle.fd_, wlocation, NULL, FILE_BEGIN);
    }

    uint64_t file_system_t::file_pointer(file_handle_t& handle) {
        return handle.cast<windows_file_handle_t>().position_;
    }

    static DWORD FSInternalRead(file_handle_t& handle, HANDLE hFile, void* buffer, int64_t nr_bytes, uint64_t location) {
        DWORD bytes_read = 0;
        OVERLAPPED ov = {};
        ov.Internal = 0;
        ov.InternalHigh = 0;
        ov.Offset = location & 0xFFFFFFFF;
        ov.OffsetHigh = location >> 32;
        ov.hEvent = 0;
        auto rc = ReadFile(hFile, buffer, (DWORD)nr_bytes, &bytes_read, &ov);
        if (!rc) {
            return DWORD();
        }
        return bytes_read;
    }

    bool file_system_t::read(file_handle_t& handle, void* buffer, int64_t nr_bytes, uint64_t location) {
        HANDLE hFile = ((windows_file_handle_t &)handle).fd_;
        auto bytes_read = FSInternalRead(handle, hFile, buffer, nr_bytes, location);
        if (bytes_read != nr_bytes) {
            return false;
        }
        return true;
    }

    int64_t file_system_t::read(file_handle_t& handle, void* buffer, int64_t nr_bytes) {
        HANDLE hFile = handle.cast<windows_file_handle_t>().fd_;
        auto& pos = handle.cast<windows_file_handle_t>().position_;
        auto n = std::min<uint64_t>(std::max<uint64_t>(file_size(handle), pos) - pos, nr_bytes);
        auto bytes_read = FSInternalRead(handle, hFile, buffer, n, pos);
        pos += bytes_read;
        return bytes_read;
    }

    static DWORD FSInternalWrite(file_handle_t& handle, HANDLE hFile, void* buffer, int64_t nr_bytes, uint64_t location) {
        DWORD bytes_written = 0;
        OVERLAPPED ov = {};
        ov.Internal = 0;
        ov.InternalHigh = 0;
        ov.Offset = location & 0xFFFFFFFF;
        ov.OffsetHigh = location >> 32;
        ov.hEvent = 0;
        auto rc = WriteFile(hFile, buffer, (DWORD)nr_bytes, &bytes_written, &ov);
        if (!rc) {
            return DWORD();
        }
        return bytes_written;
    }

    static int64_t FSWrite(file_handle_t& handle, HANDLE hFile, void* buffer, int64_t nr_bytes, uint64_t location) {
        int64_t bytes_written = 0;
        while (nr_bytes > 0) {
            auto bytes_to_write = std::min<uint64_t>(uint64_t(std::numeric_limits<int32_t>::max()), uint64_t(nr_bytes));
            DWORD current_bytes_written = FSInternalWrite(handle, hFile, buffer, bytes_to_write, location);
            if (current_bytes_written <= 0) {
                return current_bytes_written;
            }
            bytes_written += current_bytes_written;
            buffer = buffer + current_bytes_written;
            location += current_bytes_written;
            nr_bytes -= current_bytes_written;
        }
        return bytes_written;
    }

    bool file_system_t::write(file_handle_t& handle, void* buffer, int64_t nr_bytes, uint64_t location) {
        HANDLE hFile = handle.cast<windows_file_handle_t>().fd_;
        auto bytes_written = FSWrite(handle, hFile, buffer, nr_bytes, location);
        if (bytes_written != nr_bytes) {
            return false;
        }
        return true;
    }

    int64_t file_system_t::write(file_handle_t& handle, void* buffer, int64_t nr_bytes) {
        HANDLE hFile = handle.cast<windows_file_handle_t>().fd_;
        auto &pos = handle.cast<windows_file_handle_t>().position_;
        auto bytes_written = FSWrite(handle, hFile, buffer, nr_bytes, pos);
        pos += bytes_written;
        return bytes_written;
    }

    int64_t file_system_t::file_size(file_handle_t& handle) {
        HANDLE hFile = handle.cast<windows_file_handle_t>().fd_;
        LARGE_INTEGER result;
        if (!GetFileSizeEx(hFile, &result)) {
            return -1;
        }
        return result.QuadPart;
    }

    time_t file_system_t::last_modified_time(file_handle_t& handle) {
        HANDLE hFile = handle.cast<windows_file_handle_t>().fd_;

        // https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getfiletime
        FILETIME last_write;
        if (GetFileTime(hFile, nullptr, nullptr, &last_write) == 0) {
            return -1;
        }

        // https://stackoverflow.com/questions/29266743/what-is-dwlowdatetime-and-dwhighdatetime
        ULARGE_INTEGER ul;
        ul.LowPart = last_write.dwLowDateTime;
        ul.HighPart = last_write.dwHighDateTime;
        int64_t fileTime64 = ul.QuadPart;

        // fileTime64 contains a 64-bit value representing the number of
        // 100-nanosecond intervals since January 1, 1601 (UTC).
        // https://docs.microsoft.com/en-us/windows/win32/api/minwinbase/ns-minwinbase-filetime

        // Adapted from: https://stackoverflow.com/questions/6161776/convert-windows-filetime-to-second-in-unix-linux
        const auto WINDOWS_TICK = 10000000;
        const auto SEC_TO_UNIX_EPOCH = 11644473600LL;
        time_t result = (fileTime64 / WINDOWS_TICK - SEC_TO_UNIX_EPOCH);
        return result;
    }

    bool file_system_t::truncate(file_handle_t& handle, int64_t new_size) {
        HANDLE hFile = handle.cast<windows_file_handle_t>().fd_;
        // seek to the location
        set_file_pointer(handle, new_size);
        // now set the end of file position
        if (!SetEndOfFile(hFile)) {
            return false;
        }
        return true;
    }

    static DWORD WindowsGetFileAttributes(const std::string& filename) {
        auto unicode_path = string_utils::UTF8_to_Unicode(filename.c_str());
        return GetFileAttributesW(unicode_path.c_str());
    }

    bool file_system_t::directory_exists(const std::string& directory) {
        DWORD attrs = WindowsGetFileAttributes(directory);
        return (attrs != INVALID_FILE_ATTRIBUTES && (attrs & FILE_ATTRIBUTE_DIRECTORY));
    }

    bool file_system_t::create_directory(const std::string& directory) {
        if (directory_exists(directory)) {
            return true;
        }
        auto unicode_path = string_utils::UTF8_to_Unicode(directory.c_str());
        if (directory.empty() || !CreateDirectoryW(unicode_path.c_str(), NULL) || !directory_exists(directory)) {
            return false;
        }
        return true;
    }

    static bool delete_directory_recursive(base_file_system_t& fs, std::string directory) {
        fs.list_files(directory, [&](const std::string& fname, bool is_directory) {
            if (is_directory) {
                delete_directory_recursive(fs, fs.join_path(directory, fname));
            } else {
                fs.remove_file(fs.join_path(directory, fname));
            }
        });
        auto unicode_path = string_utils::UTF8_to_Unicode(directory.c_str());
        if (!RemoveDirectoryW(unicode_path.c_str())) {
            return false;
        }
        return true;
    }

    bool file_system_t::remove_directory(const std::string& directory) {
        if (file_exists(directory)) {
            return false;
        }
        if (!directory_exists(directory)) {
            return true;
        }
        return delete_directory_recursive(*this, directory.c_str());
    }

    bool file_system_t::remove_file(const std::string& filename) {
        auto unicode_path = string_utils::UTF8_to_Unicode(filename.c_str());
        if (!DeleteFileW(unicode_path.c_str())) {
            auto error = file_system_t::last_error_as_string();
            return false;
        }
        return true;
    }

    bool file_system_t::list_files(const std::string& directory, const std::function<void(const std::string&, bool)>& callback) {
        std::string search_dir = join_path(directory, "*");

        auto unicode_path = string_utils::UTF8_to_Unicode(search_dir.c_str());

        WIN32_FIND_DATAW ffd;
        HANDLE hFind = FindFirstFileW(unicode_path.c_str(), &ffd);
        if (hFind == INVALID_HANDLE_VALUE) {
            return false;
        }
        do {
            std::string cFileName = string_utils::Unicode_to_UTF8(ffd.cFileName);
            if (cFileName == "." || cFileName == "..") {
                continue;
            }
            callback(cFileName, ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY);
        } while (FindNextFileW(hFind, &ffd) != 0);

        DWORD dwError = GetLastError();
        if (dwError != ERROR_NO_MORE_FILES) {
            FindClose(hFind);
            return false;
        }

        FindClose(hFind);
        return true;
    }

    bool file_system_t::file_sync(file_handle_t& handle) {
        HANDLE hFile = handle.cast<windows_file_handle_t>().fd_;
        if (FlushFileBuffers(hFile) == 0) {
            return false;
        }
        return true;
    }

    bool file_system_t::move_files(const std::string& source, const std::string& target) {
        auto source_unicode = string_utils::UTF8_to_Unicode(source.c_str());
        auto target_unicode = string_utils::UTF8_to_Unicode(target.c_str());
        if (!MoveFileW(source_unicode.c_str(), target_unicode.c_str())) {
            return false;
        }
        return true;
    }

    file_type_t file_system_t::file_type(file_handle_t& handle) {
        auto path = handle.cast<windows_file_handle_t>().path_;
        // pipes in windows are just files in '\\.\pipe\' folder
        if (strncmp(path.c_str(), PIPE_PREFIX, strlen(PIPE_PREFIX)) == 0) {
            return file_type_t::FILE_TYPE_FIFO;
        }
        DWORD attrs = WindowsGetFileAttributes(path.c_str());
        if (attrs != INVALID_FILE_ATTRIBUTES) {
            if (attrs & FILE_ATTRIBUTE_DIRECTORY) {
                return file_type_t::FILE_TYPE_DIR;
            } else {
                return file_type_t::FILE_TYPE_REGULAR;
            }
        }
        return file_type_t::FILE_TYPE_INVALID;
    }
    #endif

    bool file_system_t::can_seek() {
        return true;
    }

    bool file_system_t::seek(file_handle_t& handle, uint64_t location) {
        if (!can_seek()) {
            return false;
        }
        set_file_pointer(handle, location);
        return true;
    }

    uint64_t file_system_t::seek_position(file_handle_t& handle) {
        if (!can_seek()) {
            return INVALID_INDEX;
        }
        return file_pointer(handle);
    }

    static bool is_crawl(const std::string& glob) {
        // glob must match exactly
        return glob == "**";
    }
    static bool has_multiple_crawl(const std::vector<std::string>& splits) {
        return std::count(splits.begin(), splits.end(), "**") > 1;
    }
    static bool is_symbolic_link(const std::string& path) {
    #ifndef _WIN32
        struct stat status;
        return (lstat(path.c_str(), &status) != -1 && S_ISLNK(status.st_mode));
    #else
        auto attributes = WindowsGetFileAttributes(path);
        if (attributes == INVALID_FILE_ATTRIBUTES)
            return false;
        return attributes & FILE_ATTRIBUTE_REPARSE_POINT;
    #endif
    }

    static void recursive_glob_directories(base_file_system_t& fs, const std::string& path, std::vector<std::string>& result, bool match_directory,
                                        bool join_path) {
        fs.list_files(path, [&](const std::string& fname, bool is_directory) {
            std::string concat;
            if (join_path) {
                concat = fs.join_path(path, fname);
            } else {
                concat = fname;
            }
            if (is_symbolic_link(concat)) {
                return;
            }
            if (is_directory == match_directory) {
                result.push_back(concat);
            }
            if (is_directory) {
                recursive_glob_directories(fs, concat, result, match_directory, true);
            }
        });
    }

    static void glob_files_internal(base_file_system_t& fs, const std::string& path, const std::string& glob, bool match_directory,
                                std::vector<std::string>& result, bool join_path) {
        fs.list_files(path, [&](const std::string& fname, bool is_directory) {
            if (is_directory != match_directory) {
                return;
            }
            if (string_utils::glob(fname.c_str(), fname.size(), glob.c_str(), glob.size(), true)) {
                if (join_path) {
                    result.push_back(fs.join_path(path, fname));
                } else {
                    result.push_back(fname);
                }
            }
        });
    }

    std::vector<std::string> file_system_t::fetch_file_without_glob(const std::string& path, bool absolute_path) {
        std::vector<std::string> result;
        if (file_exists(path) || is_pipe(path)) {
            result.push_back(path);
        } else if (!absolute_path) {
            std::vector<std::string> search_paths = string_utils::split(base_file_system_t::file_search_path_, ',');
            for (const auto &search_path : search_paths) {
                auto joined_path = join_path(search_path, path);
                if (file_exists(joined_path) || is_pipe(joined_path)) {
                    result.push_back(joined_path);
                }
            }
        }
        return result;
    }

    std::vector<std::string> file_system_t::glob_files(const std::string& path) {
        if (path.empty()) {
            return std::vector<std::string>();
        }
        // split up the path into separate chunks
        std::vector<std::string> splits;
        uint64_t last_pos = 0;
        for (uint64_t i = 0; i < path.size(); i++) {
            if (path[i] == '\\' || path[i] == '/') {
                if (i == last_pos) {
                    // empty: skip this position
                    last_pos = i + 1;
                    continue;
                }
                if (splits.empty()) {
                    splits.push_back(path.substr(0, i));
                } else {
                    splits.push_back(path.substr(last_pos, i - last_pos));
                }
                last_pos = i + 1;
            }
        }
        splits.push_back(path.substr(last_pos, path.size() - last_pos));
        // handle absolute paths
        bool absolute_path = false;
        if (path[0] == '/') {
            // first character is a slash -  unix absolute path
            absolute_path = true;
        } else if (string_utils::contains(splits[0], ":")) {
            // first split has a colon -  windows absolute path
            absolute_path = true;
        } else if (splits[0] == "~") {
            // starts with home directory
            if (!base_file_system_t::home_directory_.empty()) {
                absolute_path = true;
                splits[0] = base_file_system_t::home_directory_;
                assert(path[0] == '~');
                if (!has_glob(path)) {
                    return glob_files(base_file_system_t::home_directory_ + path.substr(1));
                }
            }
        }
        // Check if the path has a glob at all
        if (!has_glob(path)) {
            // no glob: return only the file (if it exists or is a pipe)
            return fetch_file_without_glob(path, absolute_path);
        }
        std::vector<std::string> previous_directories;
        if (absolute_path) {
            // for absolute paths, we don't start by scanning the current directory
            previous_directories.push_back(splits[0]);
        } else {
            std::vector<std::string> search_paths = string_utils::split(base_file_system_t::file_search_path_, ',');
            for (const auto &search_path : search_paths) {
                previous_directories.push_back(search_path);
            }
        }

        if (has_multiple_crawl(splits)) {
            return {};
        }

        for (uint64_t i = absolute_path ? 1 : 0; i < splits.size(); i++) {
            bool is_last_chunk = i + 1 == splits.size();
            // if it's the last chunk we need to find files, otherwise we find directories
            // not the last chunk: gather a list of all directories that match the glob pattern
            std::vector<std::string> result;
            if (!has_glob(splits[i])) {
                // no glob, just append as-is
                if (previous_directories.empty()) {
                    result.push_back(splits[i]);
                } else {
                    if (is_last_chunk) {
                        for (auto &prev_directory : previous_directories) {
                            const std::string filename = join_path(prev_directory, splits[i]);
                            if (file_exists(filename) || directory_exists(filename)) {
                                result.push_back(filename);
                            }
                        }
                    } else {
                        for (auto &prev_directory : previous_directories) {
                            result.push_back(join_path(prev_directory, splits[i]));
                        }
                    }
                }
            } else {
                if (is_crawl(splits[i])) {
                    if (!is_last_chunk) {
                        result = previous_directories;
                    }
                    if (previous_directories.empty()) {
                        recursive_glob_directories(*this, ".", result, !is_last_chunk, false);
                    } else {
                        for (auto &prev_dir : previous_directories) {
                            recursive_glob_directories(*this, prev_dir, result, !is_last_chunk, true);
                        }
                    }
                } else {
                    if (previous_directories.empty()) {
                        // no previous directories: list in the current path
                        glob_files_internal(*this, ".", splits[i], !is_last_chunk, result, false);
                    } else {
                        // previous directories
                        // we iterate over each of the previous directories, and apply the glob of the current directory
                        for (auto &prev_directory : previous_directories) {
                            glob_files_internal(*this, prev_directory, splits[i], !is_last_chunk, result, true);
                        }
                    }
                }
            }
            if (result.empty()) {
                // no result found that matches the glob
                // last ditch effort: search the path as a std::string literal
                return fetch_file_without_glob(path, absolute_path);
            }
            if (is_last_chunk) {
                return result;
            }
            previous_directories = std::move(result);
        }
        return std::vector<std::string>();
    }

    std::unique_ptr<base_file_system_t> base_file_system_t::create_local_system() {
        return std::make_unique<file_system_t>();
    }

} // namespace core::file

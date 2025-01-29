#pragma once

#include "local_file_system.hpp"
#include "virtual_file_system.hpp"

namespace core::filesystem {

    struct path_hash {
        size_t operator()(const std::filesystem::path& p) const noexcept { return std::filesystem::hash_value(p); }
    };

    template<class FSC>
    class file_system final : private FSC {
    public:
        file_system(const FSC& fs)
            : FSC(fs) {}
    };

    template<class FSC, class... Args>
    std::unique_ptr<file_handle_t> open_file(file_system<FSC>& fs, Args&&... args) {
        return open_file(fs, std::forward<Args>(args)...);
    }

    template<class FSC>
    bool read(file_system<FSC>& fs, file_handle_t& handle, void* buffer, int64_t nr_bytes, uint64_t location) {
        return read(fs, handle, buffer, nr_bytes, location);
    }

    template<class FSC>
    int64_t read(file_system<FSC>& fs, file_handle_t& handle, void* buffer, int64_t nr_bytes) {
        return read(fs, handle, buffer, nr_bytes);
    }

    template<class FSC>
    bool write(file_system<FSC>& fs, file_handle_t& handle, void* buffer, int64_t nr_bytes, uint64_t location) {
        return write(fs, handle, buffer, nr_bytes, location);
    }

    template<class FSC>
    int64_t write(file_system<FSC>& fs, file_handle_t& handle, void* buffer, int64_t nr_bytes) {
        return write(fs, handle, buffer, nr_bytes);
    }

    template<class FSC, class... Args>
    int64_t file_size(file_system<FSC>& fs, Args&&... args) {
        return file_size(fs, std::forward<Args>(args)...);
    }

    template<class FSC, class... Args>
    time_t last_modified_time(file_system<FSC>& fs, Args&&... args) {
        return last_modified_time(fs, std::forward<Args>(args)...);
    }

    template<class FSC, class... Args>
    file_type_t file_type(file_system<FSC>& fs, Args&&... args) {
        return file_type(fs, std::forward<Args>(args)...);
    }

    template<class FSC, class... Args>
    bool truncate(file_system<FSC>& fs, Args&&... args) {
        return truncate(fs, std::forward<Args>(args)...);
    }

    template<class FSC, class... Args>
    bool trim(file_system<FSC>& fs, Args&&... args) {
        return trim(fs, std::forward<Args>(args)...);
    }

    template<class FSC, class... Args>
    bool directory_exist(file_system<FSC>& fs, Args&&... args) {
        return directory_exist(fs, std::forward<Args>(args)...);
    }

    template<class FSC, class... Args>
    bool create_directory(file_system<FSC>& fs, Args&&... args) {
        return create_directory(fs, std::forward<Args>(args)...);
    }

    template<class FSC, class... Args>
    bool remove_directory(file_system<FSC>& fs, Args&&... args) {
        return remove_directory(fs, std::forward<Args>(args)...);
    }

    template<class FSC, class... Args>
    bool list_files(file_system<FSC>& fs, Args&&... args) {
        return list_files(fs, std::forward<Args>(args)...);
    }

    template<class FSC, class... Args>
    bool move_files(file_system<FSC>& fs, Args&&... args) {
        return move_files(fs, std::forward<Args>(args)...);
    }

    template<class FSC, class... Args>
    bool file_exists(file_system<FSC>& fs, Args&&... args) {
        return file_exists(fs, std::forward<Args>(args)...);
    }

    template<class FSC, class... Args>
    bool is_pipe(file_system<FSC>& fs, Args&&... args) {
        return is_pipe(fs, std::forward<Args>(args)...);
    }

    template<class FSC, class... Args>
    bool remove_file(file_system<FSC>& fs, Args&&... args) {
        return remove_file(fs, std::forward<Args>(args)...);
    }

    template<class FSC, class... Args>
    bool file_sync(file_system<FSC>& fs, Args&&... args) {
        return file_sync(fs, std::forward<Args>(args)...);
    }

    template<class FSC, class... Args>
    std::vector<path_t> glob_files(file_system<FSC>& fs, Args&&... args) {
        return glob_files(fs, std::forward<Args>(args)...);
    }

    template<class FSC, class... Args>
    bool seek(file_system<FSC>& fs, Args&&... args) {
        return seek(fs, std::forward<Args>(args)...);
    }

    template<class FSC, class... Args>
    uint64_t seek_position(file_system<FSC>& fs, Args&&... args) {
        return seek_position(fs, std::forward<Args>(args)...);
    }

#ifdef PLATFORM_WINDOWS
    template<class FSC, class... Args>
    std::string last_error_as_string(file_system<FSC>& fs, Args&&... args) {
        return last_error_as_string(fs, std::forward<Args>(args)...);
    }
#endif

} // namespace core::filesystem
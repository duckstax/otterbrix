#pragma once

#include "base_file_system.hpp"

namespace core::file {

    class file_system_t : public base_file_system_t {
    public:
        std::unique_ptr<file_handle_t> open_file(const std::string& path, uint8_t flags, file_lock_type lock = file_lock_type::NO_LOCK) override;

        // read exactly nr_bytes from the specified location in the file. Fails if nr_bytes could not be read. This is
        // equivalent to calling set_file_pointer(location) followed by calling read().
        bool read(file_handle_t& handle, void* buffer, int64_t nr_bytes, uint64_t location) override;
        // write exactly nr_bytes to the specified location in the file. Fails if nr_bytes could not be written. This is
        // equivalent to calling set_file_pointer(location) followed by calling write().
        bool write(file_handle_t& handle, void* buffer, int64_t nr_bytes, uint64_t location) override;
        // read nr_bytes from the specified file into the buffer, moving the file pointer forward by nr_bytes. Returns the
        // amount of bytes read.
        int64_t read(file_handle_t& handle, void* buffer, int64_t nr_bytes) override;
        // write nr_bytes from the buffer into the file, moving the file pointer forward by nr_bytes.
        int64_t write(file_handle_t& handle, void* buffer, int64_t nr_bytes) override;

        // Returns the file size of a file handle, returns -1 on error
        int64_t file_size(file_handle_t& handle) override;
        // Returns the file last modified time of a file handle, returns timespec with zero on all attributes on error
        time_t last_modified_time(file_handle_t& handle) override;
        // Returns the file last modified time of a file handle, returns timespec with zero on all attributes on error
        file_type_t file_type(file_handle_t& handle) override;
        // truncate a file to a maximum size of new_size, new_size should be smaller than or equal to the current size of
        // the file
        bool truncate(file_handle_t& handle, int64_t new_size) override;

        // Check if a directory exists
        bool directory_exists(const std::string& directory) override;
        // Create a directory if it does not exist
        bool create_directory(const std::string& directory) override;
        // Recursively remove a directory and all files in it
        bool remove_directory(const std::string& directory) override;
        // List files in a directory, invoking the callback method for each one with (filename, is_dir)
        bool list_files(const std::string& directory, const std::function<void(const std::string&, bool)>& callback) override;
        // Move a file from source path to the target, StorageManager relies on this being an atomic action for ACID
        // properties
        bool move_files(const std::string& source, const std::string& target) override;
        // Check if a file exists
        bool file_exists(const std::string& filename) override;

        // Check if path is a pipe
        bool is_pipe(const std::string& filename) override;
        // Remove a file from disk
        bool remove_file(const std::string& filename) override;
        // sync a file handle to disk
        bool file_sync(file_handle_t& handle) override;

        // Runs a glob on the file system, returning a list of matching files
        std::vector<std::string> glob_files(const std::string& path) override;

        bool can_handle_files(const std::string&) override {
            // Whether or not a sub-system can handle a specific file path
            return false;
        }

        // Set the file pointer of a file handle to a specified location. Reads and writes will happen from this location
        bool seek(file_handle_t& handle, uint64_t location) override;
        // Return the current seek posiiton in the file.
        uint64_t seek_position(file_handle_t& handle) override;

        // Whether or not we can seek into the file
        bool can_seek() override;

        std::string name() const override {
            return "file_system_t";
        }

        // Returns the last Win32 error, in std::string format. Returns an empty std::string if there is no error, or on non-Windows
        // systems.
        static std::string last_error_as_string();

    private:
        // Set the file pointer of a file handle to a specified location. Reads and writes will happen from this location
        bool set_file_pointer(file_handle_t& handle, uint64_t location);
        uint64_t file_pointer(file_handle_t& handle);

        std::vector<std::string> fetch_file_without_glob(const std::string& path, bool absolute_path);
    };

} // namespace core::file

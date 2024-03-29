#pragma once

#include <functional>
#include <cassert>
#include <memory>

#if defined(_WIN32) || defined(_WIN64)
#define PLATFORM_WINDOWS
#elif defined(__unix__) || defined(__unix) || (defined(__APPLE__) && defined(__MACH__))
#define PLATFORM_POSIX
#endif

#undef create_directory
#undef move_files
#undef remove_directory

namespace core::file {

    // The value used to signify an invalid index entry
    static constexpr const uint64_t INVALID_INDEX = uint64_t(-1);

    class base_file_system_t;

    enum class file_type_t {
        // Regular file
        FILE_TYPE_REGULAR,
        // Directory
        FILE_TYPE_DIR,
        // FIFO named pipe
        FILE_TYPE_FIFO,
        // Socket
        FILE_TYPE_SOCKET,
        // Symbolic link
        FILE_TYPE_LINK,
        // Block device
        FILE_TYPE_BLOCKDEV,
        // Character device
        FILE_TYPE_CHARDEV,
        // Unknown or invalid file handle
        FILE_TYPE_INVALID
    };

    struct file_handle_t {
    public:
        file_handle_t(base_file_system_t& file_system, std::string path);
        file_handle_t(const file_handle_t&) = delete;
        virtual ~file_handle_t();

        int64_t read(void* buffer, uint64_t nr_bytes);
        int64_t write(void* buffer, uint64_t nr_bytes);
        void read(void* buffer, uint64_t nr_bytes, uint64_t location);
        void write(void* buffer, uint64_t nr_bytes, uint64_t location);
        bool seek(uint64_t location);
        void reset();
        uint64_t seek_position();
        void sync();
        bool truncate(int64_t new_size);
        std::string read_line();

        bool can_seek();
        bool is_pipe();
        uint64_t file_size();
        file_type_t type();

        // Closes the file handle.
        virtual void close() = 0;

        std::string path() const {
            return path_;
        }

        template <class TARGET>
        TARGET& cast() {
            assert(dynamic_cast<TARGET*>(this));
            return reinterpret_cast<TARGET&>(*this);
        }
        template <class TARGET>
        const TARGET& cast() const {
            assert(dynamic_cast<const TARGET*>(this));
            return reinterpret_cast<const TARGET&>(*this);
        }

    public:
        base_file_system_t &file_system_;
        std::string path_;
    };

    enum class file_lock_type : uint8_t { NO_LOCK = 0, READ_LOCK = 1, WRITE_LOCK = 2 };

    class file_flags {
    public:
        // Open file with read access
        static constexpr uint8_t FILE_FLAGS_READ = 1 << 0;
        // Open file with write access
        static constexpr uint8_t FILE_FLAGS_WRITE = 1 << 1;
        // Use direct IO when reading/writing to the file
        static constexpr uint8_t FILE_FLAGS_DIRECT_IO = 1 << 2;
        // Create file if not exists, can only be used together with WRITE
        static constexpr uint8_t FILE_FLAGS_FILE_CREATE = 1 << 3;
        // Always create a new file. If a file exists, the file is truncated. Cannot be used together with CREATE.
        static constexpr uint8_t FILE_FLAGS_FILE_CREATE_NEW = 1 << 4;
        // Open file in append mode
        static constexpr uint8_t FILE_FLAGS_APPEND = 1 << 5;
    };

    class base_file_system_t {
    public:
        virtual ~base_file_system_t();
        
        static constexpr file_lock_type DEFAULT_LOCK = file_lock_type::NO_LOCK;

        virtual std::unique_ptr<file_handle_t> open_file(const std::string& path, uint8_t flags,
                                                        file_lock_type lock = DEFAULT_LOCK) = 0;

        // read exactly nr_bytes from the specified location in the file. Fails if nr_bytes could not be read. This is
        // equivalent to calling set_file_pointer(location) followed by calling read().
        virtual bool read(file_handle_t& handle, void* buffer, int64_t nr_bytes, uint64_t location) = 0;
        // read nr_bytes from the specified file into the buffer, moving the file pointer forward by nr_bytes. Returns the
        // amount of bytes read.
        virtual int64_t read(file_handle_t& handle, void* buffer, int64_t nr_bytes) = 0;
        // write exactly nr_bytes to the specified location in the file. Fails if nr_bytes could not be written. This is
        // equivalent to calling set_file_pointer(location) followed by calling write().
        virtual bool write(file_handle_t& handle, void* buffer, int64_t nr_bytes, uint64_t location) = 0;
        // write nr_bytes from the buffer into the file, moving the file pointer forward by nr_bytes.
        virtual int64_t write(file_handle_t& handle, void* buffer, int64_t nr_bytes) = 0;

        // Returns the file size of a file handle, returns -1 on error
        virtual int64_t file_size(file_handle_t& handle) = 0;
        // Returns the file last modified time of a file handle, returns timespec with zero on all attributes on error
        virtual time_t last_modified_time(file_handle_t& handle) = 0;
        // Returns the file last modified time of a file handle, returns timespec with zero on all attributes on error
        virtual file_type_t file_type(file_handle_t& handle) = 0;
        // truncate a file to a maximum size of new_size, new_size should be smaller than or equal to the current size of
        // the file
        virtual bool truncate(file_handle_t& handle, int64_t new_size) = 0;

        // Check if a directory exists
        virtual bool directory_exists(const std::string& directory) = 0;
        // Create a directory if it does not exist
        virtual bool create_directory(const std::string& directory) = 0;
        // Recursively remove a directory and all files in it
        virtual bool remove_directory(const std::string& directory) = 0;
        // List files in a directory, invoking the callback method for each one with (filename, is_dir)
        virtual bool list_files(const std::string& directory,
                                        const std::function<void(const std::string&, bool)>& callback) = 0;

        // Move a file from source path to the target, StorageManager relies on this being an atomic action for ACID
        // properties
        virtual bool move_files(const std::string& source, const std::string& target) = 0;
        // Check if a file exists
        virtual bool file_exists(const std::string& filename) = 0;
        // Check if path is pipe
        virtual bool is_pipe(const std::string& filename) = 0;
        // Remove a file from disk
        virtual bool remove_file(const std::string& filename) = 0;
        // sync a file handle to disk
        virtual bool file_sync(file_handle_t& handle) = 0;
        // Sets the working directory
        static bool set_working_directory(const std::string& path);
        // Gets the working directory
        static std::string working_directory();
        // Gets the users home directory
        virtual std::string home_directory();
        // Expands a given path, including e.g. expanding the home directory of the user
        virtual std::string expand_path(const std::string& path);
        // Returns the system-available memory in bytes. Returns INVALID_INDEX if the system function fails.
        static uint64_t available_memory();
        // Path separator for path
        virtual std::string path_separator();
        // Checks if path is starts with separator (i.e., '/' on UNIX '\\' on Windows)
        bool is_path_absolute(const std::string& path);
        // Normalize an absolute path - the goal of normalizing is converting "\test.db" and "C:/test.db" into "C:\test.db"
        // so that the database system cache can correctly
        std::string normalize_path_absolute(const std::string& path);
        // join two paths together
        std::string join_path(const std::string& a, const std::string& path);
        // Convert separators in a path to the local separators (e.g. convert "/" into \\ on windows)
        std::string convert_separators(const std::string& path);
        // Extract the base name of a file (e.g. if the input is lib/example.dll the base name is 'example')
        std::string extract_base_name(const std::string& path);
        // Extract the name of a file (e.g if the input is lib/example.dll the name is 'example.dll')
        std::string extract_name(const std::string& path);

        // Returns the value of an environment variable - or the empty std::string if it is not set
        static std::string enviroment_variable(const std::string& name);

        // Whether there is a glob in the std::string
        static bool has_glob(const std::string& str);
        // Runs a glob on the file system, returning a list of matching files
        virtual std::vector<std::string> glob_files(const std::string& path) = 0;

        // Whether or not a sub-system can handle a specific file path
        virtual bool can_handle_files(const std::string& fpath) = 0;

        // Set the file pointer of a file handle to a specified location. Reads and writes will happen from this location
        virtual bool seek(file_handle_t& handle, uint64_t location) = 0;
        // reset a file to the beginning (equivalent to seek(handle, 0) for simple files)
        virtual void reset(file_handle_t& handle);
        virtual uint64_t seek_position(file_handle_t& handle) = 0;

        // Whether or not we can seek into the file
        virtual bool can_seek() = 0;

        // Create a file_system_t.
        static std::unique_ptr<base_file_system_t> create_local_system();

        // Return the name of the filesytem. Used for forming diagnosis messages.
        virtual std::string name() const = 0;

    protected:
        std::string home_directory_;
        std::string file_search_path_;
    };

} // namespace core::file

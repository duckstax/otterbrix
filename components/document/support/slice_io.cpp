#include "slice_io.hpp"

#if FL_HAVE_FILESYSTEM

#include "exception.hpp"
#include "platform_compat.hpp"
#include "num_conversion.hpp"
#include <fcntl.h>
#include <errno.h>

#ifndef _MSC_VER
#include <sys/stat.h>
#include <unistd.h>
#define _open open
#define _close close
#define _write write
#define _read read
#define O_BINARY 0
#else
#include <io.h>
#include <windows.h>
#endif

namespace storage {

alloc_slice_t read_file(const char *path) {
    int fd = ::_open(path, O_RDONLY | O_BINARY);
    if (fd < 0)
        exception_t::_throw_errno("Can't open file %s", path);
    struct stat stat;
    fstat(fd, &stat);
    if (static_cast<size_t>(stat.st_size) > SIZE_MAX)
        throw std::logic_error("File too big for address space");
    alloc_slice_t data(static_cast<size_t>(stat.st_size));
    size_t bytes_read = static_cast<size_t>(::_read(fd, (void*)data.buf, static_cast<unsigned int>(data.size)));
    if (bytes_read < static_cast<size_t>(data.size))
        exception_t::_throw_errno("Can't read file %s", path);
    ::_close(fd);
    return data;
}

void write_to_file(slice_t s, const char *path, int mode) {
    int fd = ::_open(path, mode | O_WRONLY | O_BINARY, 0600);
    if (fd < 0)
        exception_t::_throw_errno("Can't open file");
    size_t written = static_cast<size_t>(::_write(fd, s.buf, static_cast<unsigned int>(s.size)));
    if(written < static_cast<size_t>(s.size))
        exception_t::_throw_errno("Can't write file");
    ::_close(fd);
}

void write_to_file(slice_t s, const char *path) {
    write_to_file(s, path, O_CREAT | O_TRUNC);
}

void append_to_file(slice_t s, const char *path) {
    write_to_file(s, path, O_CREAT | O_APPEND);
}

}

#endif

#pragma once

#include "platform_compat.hpp"
#include <functional>
#include <iosfwd>
#include <memory>
#include <string>
#include <typeinfo>
#include <vector>

namespace storage {

class backtrace_t
{
public:

    struct frame_info_t
    {
        const void* pc;
        size_t offset;
        const char *function;
        const char *library;
    };

    static std::shared_ptr<backtrace_t> capture(unsigned skip_frames = 0, unsigned max_frames = 50);

    explicit backtrace_t(unsigned skip_frames = 0, unsigned max_frames = 50);
    void skip(unsigned n_frames);
    bool write_to(std::ostream &out) const;
    std::string to_string() const;

    unsigned size() const;
    frame_info_t get_frame(unsigned i) const;
    frame_info_t operator[] (unsigned i);
    static void install_terminate_handler(std::function<void(const std::string&)> logger);

private:
    void _capture(unsigned skip_frames = 0, unsigned max_frames = 50);
    char* print_frame(unsigned i) const;
    static void write_crash_log(std::ostream &out);

    std::vector<void*> _addrs;
};


std::string unmangle(const std::type_info &type);
std::string unmangle(const char *name NONNULL);
std::string function_name(const void *pc);

}

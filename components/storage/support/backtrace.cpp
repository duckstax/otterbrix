#include "backtrace.hpp"
#include <csignal>
#include <exception>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string.h>
#include <algorithm>
#include "better_assert.hpp"

#ifndef _MSC_VER

#include <dlfcn.h>
#include <stdlib.h>
#include <unwind.h>

namespace storage {

static constexpr const char* terminal_functions[] = {
    "_C_A_T_C_H____T_E_S_T_",
    "Catch::TestInvokerAsFunction::invoke() const",
    "litecore::actor::Scheduler::task(unsigned)",
    "litecore::actor::GCDMailbox::safelyCall",
};

static constexpr struct {const char *old, *nuu;} abbreviations[] = {
{"(anonymous namespace)",   "(anon)"},
{"std::__1::",              "std::"},
{"std::basic_string<char, std::char_traits<char>, std::allocator<char> >",
    "std::string"},
};


#ifndef HAVE_EXECINFO
struct backtrace_state_t
{
    void** current;
    void** end;
};


static _Unwind_Reason_Code _unwind_callback(struct _Unwind_Context* context, void* arg) {
    backtrace_state_t* state = static_cast<backtrace_state_t*>(arg);
    uintptr_t pc = _Unwind_GetIP(context);
    if (pc) {
        if (state->current == state->end) {
            return _URC_END_OF_STACK;
        } else {
            *state->current++ = reinterpret_cast<void*>(pc);
        }
    }
    return _URC_NO_REASON;
}

static int _backtrace(void** buffer, size_t max) {
    backtrace_state_t state = {buffer, buffer + max};
    _Unwind_Backtrace(_unwind_callback, &state);
    return int(state.current - buffer);
}
#endif


static char* _unmangle(const char *function) {
#ifdef HAVE_UNMANGLE
    int status;
    size_t unmangled_len;
    char *unmangled = abi::__cxa_demangle(function, nullptr, &unmangled_len, &status);
    if (unmangled && status == 0)
        return unmangled;
    free(unmangled);
#endif
    return (char*)function;
}

static void replace(std::string &str, std::string_view old_str, std::string_view new_str) {
    std::string::size_type pos = 0;
    while (std::string::npos != (pos = str.find(old_str, pos))) {
        str.replace(pos, old_str.size(), new_str);
        pos += new_str.size();
    }
}


backtrace_t::frame_info_t backtrace_t::get_frame(unsigned i) const {
    precondition(i < _addrs.size());
    frame_info_t frame = { };
    Dl_info info;
    if (dladdr(_addrs[i], &info)) {
        frame.pc = _addrs[i];
        frame.offset = (size_t)frame.pc - (size_t)info.dli_saddr;
        frame.function = info.dli_sname;
        frame.library = info.dli_fname;
        const char *slash = strrchr(frame.library, '/');
        if (slash)
            frame.library = slash + 1;
    }
    return frame;
}

backtrace_t::frame_info_t backtrace_t::operator[](unsigned i) {
    return get_frame(i);
}

bool backtrace_t::write_to(std::ostream &out) const {
    for (int i = 0; i < static_cast<int>(_addrs.size()); ++i) {
        if (i > 0)
            out << '\n';
        out << '\t';
        char *cstr = nullptr;
        auto frame = get_frame(i);
        int len;
        bool stop = false;
        if (frame.function) {
            std::string name = unmangle(frame.function);
            for (auto fn : terminal_functions) {
                if (name.find(fn) != std::string::npos)
                    stop = true;
            }
            for (auto &abbrev : abbreviations)
                replace(name, abbrev.old, abbrev.nuu);
            len = asprintf(&cstr, "%2d  %-25s %s + %zd",
                           i, frame.library, name.c_str(), frame.offset);
        } else {
            len = asprintf(&cstr, "%2d  %p", i, _addrs[i]);
        }
        if (len < 0)
            return false;
        out.write(cstr, size_t(len));
        free(cstr);

        if (stop) {
            out << "\n\t ... (" << (_addrs.size() - i - 1) << " more suppressed) ...";
            break;
        }
    }
    return true;
}


std::string function_name(const void *pc) {
    Dl_info info = {};
    dladdr(pc, &info);
    if (info.dli_sname)
        return unmangle(info.dli_sname);
    else
        return "";
}

}

#else

#pragma comment(lib, "Dbghelp.lib")
#include <Windows.h>
#include <Dbghelp.h>
#include "asprintf.h"
#include <sstream>
using namespace std;

namespace storage {

#if WINAPI_FAMILY_PARTITION(WINAPI_PARTITION_DESKTOP)

static inline int backtrace(void** buffer, size_t max) {
    return (int)CaptureStackBackTrace(0, (DWORD)max, buffer, nullptr);
}


bool backtrace_t::write_to(std::ostream &out) const {
    const auto process = GetCurrentProcess();
    SYMBOL_INFO *symbol = nullptr;
    IMAGEHLP_LINE64 *line = nullptr;
    bool success = false;
    SymInitialize(process, nullptr, TRUE);
    DWORD symOptions = SymGetOptions();
    symOptions |= SYMOPT_LOAD_LINES | SYMOPT_UNDNAME;
    SymSetOptions(symOptions);

    symbol = (SYMBOL_INFO*)malloc(sizeof(SYMBOL_INFO)+1023 * sizeof(TCHAR));
    if (!symbol)
        goto exit;
    symbol->MaxNameLen = 1024;
    symbol->SizeOfStruct = sizeof(SYMBOL_INFO);
    DWORD displacement;
    line = (IMAGEHLP_LINE64*)malloc(sizeof(IMAGEHLP_LINE64));
    if (!line)
        goto exit;
    line->SizeOfStruct = sizeof(IMAGEHLP_LINE64);

    for (unsigned i = 0; i < _addrs.size(); i++) {
        if (i > 0)
            out << "\r\n";
        out << '\t';
        const auto address = (DWORD64)_addrs[i];
        SymFromAddr(process, address, nullptr, symbol);
        char* cstr = nullptr;
        if (SymGetLineFromAddr64(process, address, &displacement, line)) {
            asprintf(&cstr, "at %s in %s: line: %lu: address: 0x%0llX",
                     symbol->Name, line->FileName, line->LineNumber, symbol->Address);
        } else {
            asprintf(&cstr, "at %s, address 0x%0llX",
                     symbol->Name, symbol->Address);
        }
        if (!cstr)
            goto exit;
        out << cstr;
        free(cstr);
    }
    success = true;

exit:
    free(symbol);
    free(line);
    SymCleanup(process);
    return success;
}

#else
static inline int backtrace(void** buffer, size_t max) {return 0;}
bool backtrace_t::write_to(std::ostream&) const  {return false;}
#endif


static char* unmangle(const char *function) {
    return (char*)function;
}

}

#endif


namespace storage {

std::shared_ptr<backtrace_t> backtrace_t::capture(unsigned skip_frames, unsigned max_frames) {
    auto bt = std::make_shared<backtrace_t>(0, 0);
    bt->_capture(skip_frames + 1, max_frames);
    return bt;
}

backtrace_t::backtrace_t(unsigned skip_frames, unsigned max_frames) {
    if (max_frames > 0)
        capture(skip_frames + 1, max_frames);
}

void backtrace_t::_capture(unsigned skip_frames, unsigned max_frames) {
    _addrs.resize(++skip_frames + max_frames);
    auto n = _backtrace(&_addrs[0], skip_frames + max_frames);
    _addrs.resize(n);
    skip(skip_frames);
}

void backtrace_t::skip(unsigned n_frames) {
    _addrs.erase(_addrs.begin(), _addrs.begin() + std::min(size_t(n_frames), _addrs.size()));
}

std::string backtrace_t::to_string() const {
    std::stringstream out;
    write_to(out);
    return out.str();
}

unsigned backtrace_t::size() const {
    return static_cast<unsigned>(_addrs.size());
}

void backtrace_t::write_crash_log(std::ostream &out) {
    backtrace_t bt(4);
    auto xp = std::current_exception();
    if (xp) {
        out << "Uncaught exception:\n\t";
        try {
            rethrow_exception(xp);
        } catch(const std::exception& x) {
            const char *name = typeid(x).name();
            char *unmangled = _unmangle(name);
            out << unmangled << ": " <<  x.what() << "\n";
            if (unmangled != name)
                free(unmangled);
        } catch (...) {
            out << "unknown exception type\n";
        }
    }
    out << "backtrace_t:";
    bt.write_to(out);
}

void backtrace_t::install_terminate_handler(std::function<void(const std::string&)> logger) {
    static std::once_flag once;
    call_once(once, [&] {
        static auto const st_logger = move(logger);
        static std::terminate_handler const old_handler = std::set_terminate([] {
            if (st_logger) {
                std::stringstream out;
                write_crash_log(out);
                st_logger(out.str());
            } else {
                std::cerr << "\n\n******************** C++ fatal error ********************\n";
                write_crash_log(std::cerr);
                std::cerr << "\n******************** Now terminating ********************\n";
            }
            old_handler();
            abort();
        });
    });
}


std::string unmangle(const char *name NONNULL) {
    auto unmangled = _unmangle(name);
    std::string result = unmangled;
    if (unmangled != name)
        free(unmangled);
    return result;
}

std::string unmangle(const std::type_info &type) {
    return unmangle(type.name());
}

}

#include "utils.hpp"
#include <algorithm>
#include <sstream>
#include <memory>

namespace core::file {
    
    bool string_utils::contains(const std::string& haystack, const std::string& needle) {
        return (haystack.find(needle) != std::string::npos);
    }

    bool string_utils::starts_with(std::string str, std::string prefix) {
        if (prefix.size() > str.size()) {
            return false;
        }
        return equal(prefix.begin(), prefix.end(), str.begin());
    }

    std::string string_utils::lower(const std::string& str) {
        std::string copy(str);
        std::transform(copy.begin(), copy.end(), copy.begin(), [](unsigned char c) { return string_utils::c_to_lower(c); });
        return (copy);
    }

    std::vector<std::string> string_utils::split(const std::string& str, char delimiter) {
        std::stringstream ss(str);
        std::vector<std::string> lines;
        std::string temp;
        while (std::getline(ss, temp, delimiter)) {
            lines.push_back(temp);
        }
        return (lines);
    }

    std::vector<std::string> string_utils::split(const std::string& input, const std::string& split) {
        std::vector<std::string> splits;

        uint64_t last = 0;
        uint64_t input_len = input.size();
        uint64_t split_len = split.size();
        while (last <= input_len) {
            uint64_t next = input.find(split, last);
            if (next == std::string::npos) {
                next = input_len;
            }

            // Push the substring [last, next) on to splits
            std::string substr = input.substr(last, next - last);
            if (!substr.empty()) {
                splits.push_back(substr);
            }
            last = next + split_len;
        }
        if (splits.empty()) {
            splits.push_back(input);
        }
        return splits;
    }

    std::string string_utils::replace(std::string source, const std::string& from, const std::string& to) {
        if (from.empty()) {
            throw std::invalid_argument("Invalid argument to string_utils::replace - empty FROM");
        }
        uint64_t start_pos = 0;
        while ((start_pos = source.find(from, start_pos)) != std::string::npos) {
            source.replace(start_pos, from.length(), to);
            start_pos += to.length(); // In case 'to' contains 'from', like
                                    // replacing 'x' with 'yx'
        }
        return source;
    }

    #ifdef PLATFORM_WINDOWS

        std::wstring string_utils::UTF8_to_Unicode(const char* input) {
            uint64_t result_size;

            result_size = MultiByteToWideChar(CP_UTF8, 0, input, -1, nullptr, 0);
            if (result_size == 0) {
                throw std::runtime_error("Failure in MultiByteToWideChar");
            }
            auto buffer = std::make_unique<wchar_t[]>(result_size);
            result_size = MultiByteToWideChar(CP_UTF8, 0, input, -1, buffer.get(), result_size);
            if (result_size == 0) {
                throw std::runtime_error("Failure in MultiByteToWideChar");
            }
            return std::wstring(buffer.get(), result_size);
        }

        std::string string_utils::wchar_to_MultiByteWrapper(LPCWSTR input, uint32_t code_page) {
            uint64_t result_size;

            result_size = WideCharToMultiByte(code_page, 0, input, -1, 0, 0, 0, 0);
            if (result_size == 0) {
                throw std::runtime_error("Failure in WideCharToMultiByte");
            }
            auto buffer = std::make_unique<char[]>(result_size);
            result_size = WideCharToMultiByte(code_page, 0, input, -1, buffer.get(), result_size, 0, 0);
            if (result_size == 0) {
                throw std::runtime_error("Failure in WideCharToMultiByte");
            }
            return std::string(buffer.get(), result_size - 1);
        }

        std::string string_utils::Unicode_to_UTF8(LPCWSTR input) {
            return wchar_to_MultiByteWrapper(input, CP_UTF8);
        }

        std::string string_utils::windows_Unicode_to_MBCS(LPCWSTR unicode_text, int use_ansi) {
            uint32_t code_page = use_ansi ? CP_ACP : CP_OEMCP;
            return wchar_to_MultiByteWrapper(unicode_text, code_page);
        }

        std::string string_utils::UTF8_to_MBCS(const char* input, bool use_ansi) {
            auto unicode = string_utils::UTF8_to_Unicode(input);
            return windows_Unicode_to_MBCS(unicode.c_str(), use_ansi);
        }

        #endif
}
#pragma once

#include <stdexcept>
#include <cstring>
#include <set>
#include <vector>
#include <unordered_map>

#if defined(_WIN32)
#include <windows.h>
#endif


namespace core::file {

    /**
     * String Utility Functions
     * Note that these are not the most efficient implementations (i.e., they copy
     * memory) and therefore they should only be used for debug messages and other
     * such things.
     */
    class string_utils {
    public:

        static char c_to_lower(char c) {
            if (c >= 'A' && c <= 'Z') {
                return c - ('A' - 'a');
            }
            return c;
        }

        // Returns true if the needle std::string exists in the haystack
        static bool contains(const std::string& haystack, const std::string& needle);

        // Returns true if the target std::string starts with the given prefix
        static bool starts_with(std::string str, std::string prefix);

        // split the input std::string based on newline char
        static std::vector<std::string> split(const std::string& str, char delimiter);

        // join multiple std::strings into one std::string. Components are concatenated by the given separator
        static std::string join(const std::vector<std::string>& input, const std::string& separator);
        static std::string join(const std::set<std::string>& input, const std::string& separator);

        template <class T>
        static std::string to_string(const std::vector<T>& input, const std::string& separator) {
            std::vector<std::string> input_list;
            for (auto& i : input) {
                input_list.push_back(i.to_string());
            }
            return string_utils::join(input_list, separator);
        }

        // join multiple items of container with given size, transformed to std::string
        // using function, into one std::string using the given separator
        template <typename C, typename S, typename Func>
        static std::string join(const C& input, S count, const std::string& separator, Func f) {
            // The result
            std::string result;

            // If the input isn't empty, append the first element. We do this so we
            // don't need to introduce an if into the loop.
            if (count > 0) {
                result += f(input[0]);
            }

            // Append the remaining input components, after the first
            for (size_t i = 1; i < count; i++) {
                result += separator + f(input[i]);
            }

            return result;
        }
        // Convert a std::string to lowercase
        static std::string lower(const std::string& str);

        // split the input std::string into a std::vector of std::strings based on the split std::string
        static std::vector<std::string> split(const std::string& input, const std::string& split);

        static std::string replace(std::string source, const std::string& from, const std::string& to);
            
        static bool glob(const char* string, uint64_t slen, const char* pattern, uint64_t plen, bool allow_question_mark) {
            uint64_t sidx = 0;
            uint64_t pidx = 0;
        main_loop : {
            // main matching loop
            while (sidx < slen && pidx < plen) {
                char s = string[sidx];
                char p = pattern[pidx];
                switch (p) {
                case '*': {
                    // asterisk: match any set of characters
                    // skip any subsequent asterisks
                    pidx++;
                    while (pidx < plen && pattern[pidx] == '*') {
                        pidx++;
                    }
                    // if the asterisk is the last character, the pattern always matches
                    if (pidx == plen) {
                        return true;
                    }
                    // recursively match the remainder of the pattern
                    for (; sidx < slen; sidx++) {
                        if (glob(string + sidx, slen - sidx, pattern + pidx, plen - pidx, allow_question_mark)) {
                            return true;
                        }
                    }
                    return false;
                }
                case '?':
                    // when enabled: matches anything but null
                    if (allow_question_mark) {
                        break;
                    }
                case '[':
                    pidx++;
                    goto parse_bracket;
                case '\\':
                    // escape character, next character needs to match literally
                    pidx++;
                    // check that we still have a character remaining
                    if (pidx == plen) {
                        return false;
                    }
                    p = pattern[pidx];
                    if (s != p) {
                        return false;
                    }
                    break;
                default:
                    // not a control character: characters need to match literally
                    if (s != p) {
                        return false;
                    }
                    break;
                }
                sidx++;
                pidx++;
            }
            while (pidx < plen && pattern[pidx] == '*') {
                pidx++;
            }
            // we are finished only if we have consumed the full pattern
            return pidx == plen && sidx == slen;
        }
        parse_bracket : {
            // inside a bracket
            if (pidx == plen) {
                return false;
            }
            // check the first character
            // if it is an exclamation mark we need to invert our logic
            char p = pattern[pidx];
            char s = string[sidx];
            bool invert = false;
            if (p == '!') {
                invert = true;
                pidx++;
            }
            bool found_match = invert;
            uint64_t start_pos = pidx;
            bool found_closing_bracket = false;
            // now check the remainder of the pattern
            while (pidx < plen) {
                p = pattern[pidx];
                // if the first character is a closing bracket, we match it literally
                // otherwise it indicates an end of bracket
                if (p == ']' && pidx > start_pos) {
                    // end of bracket found: we are done
                    found_closing_bracket = true;
                    pidx++;
                    break;
                }
                // we either match a range (a-b) or a single character (a)
                // check if the next character is a dash
                if (pidx + 1 == plen) {
                    // no next character!
                    break;
                }
                bool matches;
                if (pattern[pidx + 1] == '-') {
                    // range! find the next character in the range
                    if (pidx + 2 == plen) {
                        break;
                    }
                    char next_char = pattern[pidx + 2];
                    // check if the current character is within the range
                    matches = s >= p && s <= next_char;
                    // shift the pattern forward past the range
                    pidx += 3;
                } else {
                    // no range! perform a direct match
                    matches = p == s;
                    // shift the pattern forward past the character
                    pidx++;
                }
                if (found_match == invert && matches) {
                    // found a match! set the found_matches flag
                    // we keep on pattern matching after this until we reach the end bracket
                    // however, we don't need to update the found_match flag anymore
                    found_match = !invert;
                }
            }
            if (!found_closing_bracket) {
                // no end of bracket: invalid pattern
                return false;
            }
            if (!found_match) {
                // did not match the bracket: return false;
                return false;
            }
            // finished the bracket matching: move forward
            sidx++;
            goto main_loop;
        }
        }
    
        #ifdef PLATFORM_WINDOWS

        static std::wstring UTF8_to_Unicode(const char* input);

        static std::string wchar_to_MultiByteWrapper(LPCWSTR input, uint32_t code_page);

        static std::string Unicode_to_UTF8(LPCWSTR input);

        static std::string windows_Unicode_to_MBCS(LPCWSTR unicode_text, int use_ansi);

        static std::string UTF8_to_MBCS(const char* input, bool use_ansi);

        #endif
    };

} // namespace core::file
#pragma once

#include <limits>
#include <list>
#include <map>
#include <string>
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <cwchar>
#include <time.h>
#include <cwctype>
#include <cctype>

template <class char_t> struct diff_match_patch_traits {};

template <class T, class traits = diff_match_patch_traits<typename T::value_type> >
class diff_match_patch {
public:
    typedef T string_t;
    typedef typename string_t::value_type char_t;

    enum operation_t {
        remove,
        insert,
        equal
    };


    class diff_t {
    public:
        operation_t operation;
        string_t text;

        diff_t(operation_t _operation, const string_t &_text) : operation(_operation), text(_text) {}
        diff_t() = default;

        string_t to_string() const {
            string_t pretty_text = text;
            for (typename string_t::iterator i = pretty_text.begin(); i != pretty_text.end(); ++i)
                if (traits::to_wchar(*i) == L'\n') *i = traits::from_wchar(L'\u00b6');
            return traits::cs(L"diff_t(") + operation_name(operation) + traits::cs(L",\"") + pretty_text + traits::cs(L"\")");
        }

        bool operator==(const diff_t &d) const {
            return (d.operation == this->operation) && (d.text == this->text);
        }

        bool operator!=(const diff_t &d) const { return !(operator == (d)); }

        static string_t operation_name(operation_t op) {
            switch (op) {
            case operation_t::insert:
                return traits::cs(L"INSERT");
            case operation_t::remove:
                return traits::cs(L"DELETE");
            case operation_t::equal:
                return traits::cs(L"EQUAL");
            }
            throw string_t(traits::cs(L"Invalid operation."));
        }
    };
    typedef std::list<diff_t> list_diff_t;


    class patch_t {
    public:
        list_diff_t diffs;
        ssize_t start1;
        ssize_t start2;
        ssize_t length1;
        ssize_t length2;

        patch_t() : start1(0), start2(0), length1(0), length2(0) {}

        bool is_null() const {
            return start1 == 0 && start2 == 0 && length1 == 0 && length2 == 0 && diffs.size() == 0;
        }

        string_t to_string() const {
            string_t coords1, coords2;
            if (length1 == 0) {
                coords1 = as_string(start1) + traits::cs(L",0");
            } else if (length1 == 1) {
                coords1 = as_string(start1 + 1);
            } else {
                coords1 = as_string(start1 + 1) + traits::from_wchar(L',') + as_string(length1);
            }
            if (length2 == 0) {
                coords2 = as_string(start2) + traits::cs(L",0");
            } else if (length2 == 1) {
                coords2 = as_string(start2 + 1);
            } else {
                coords2 = as_string(start2 + 1) + traits::from_wchar(L',') + as_string(length2);
            }
            string_t text(traits::cs(L"@@ -") + coords1 + traits::cs(L" +") + coords2 + traits::cs(L" @@\n"));
            for (typename list_diff_t::const_iterator cur_diff = diffs.begin(); cur_diff != diffs.end(); ++cur_diff) {
                switch ((*cur_diff).operation) {
                case operation_t::insert:
                    text += traits::from_wchar(L'+');
                    break;
                case operation_t::remove:
                    text += traits::from_wchar(L'-');
                    break;
                case operation_t::equal:
                    text += traits::from_wchar(L' ');
                    break;
                }
                append_percent_encoded(text, (*cur_diff).text);
                text += traits::from_wchar(L'\n');
            }

            return text;
        }
    };
    typedef std::list<patch_t> list_patch_t;

    friend class diff_match_patch_test;

public:
    float diff_timeout;
    short diff_edit_cost;
    float match_threshold;
    ssize_t match_distance;
    float patch_delete_threshold;
    short patch_margin;
    short match_max_bits;

    diff_match_patch()
        : diff_timeout(1.0f)
        , diff_edit_cost(4)
        , match_threshold(0.5f)
        , match_distance(1000)
        , patch_delete_threshold(0.5f)
        , patch_margin(4)
        , match_max_bits(32)
    {}

    list_diff_t diff_main(const string_t &text1, const string_t &text2, bool checklines = true) const {
        clock_t deadline;
        if (diff_timeout <= 0) {
            deadline = std::numeric_limits<clock_t>::max();
        } else {
            deadline = clock() + (clock_t)(diff_timeout * CLOCKS_PER_SEC);
        }
        list_diff_t diffs;
        diff_main(text1, text2, checklines, deadline, diffs);
        return diffs;
    }

private:
    static void diff_main(const string_t &text1, const string_t &text2, bool checklines, clock_t deadline, list_diff_t& diffs) {
        diffs.clear();
        if (text1 == text2) {
            if (!text1.empty()) {
                diffs.push_back(diff_t(operation_t::equal, text1));
            }
        } else {
            ssize_t common_len = diff_common_prefix(text1, text2);
            const string_t &common_prefix = text1.substr(0, common_len);
            string_t text_chopped1 = text1.substr(common_len);
            string_t text_chopped2 = text2.substr(common_len);

            common_len = diff_common_suffix(text_chopped1, text_chopped2);
            const string_t &common_suffix = right(text_chopped1, common_len);
            text_chopped1 = text_chopped1.substr(0, text_chopped1.length() - common_len);
            text_chopped2 = text_chopped2.substr(0, text_chopped2.length() - common_len);

            diff_compute(text_chopped1, text_chopped2, checklines, deadline, diffs);
            if (!common_prefix.empty()) {
                diffs.push_front(diff_t(operation_t::equal, common_prefix));
            }
            if (!common_suffix.empty()) {
                diffs.push_back(diff_t(operation_t::equal, common_suffix));
            }
            diff_cleanup_merge(diffs);
        }
    }

    static void diff_compute(string_t text1, string_t text2, bool checklines, clock_t deadline, list_diff_t& diffs) {
        if (text1.empty()) {
            diffs.push_back(diff_t(operation_t::insert, text2));
            return;
        }
        if (text2.empty()) {
            diffs.push_back(diff_t(operation_t::remove, text1));
            return;
        }
        const string_t& longtext = text1.length() > text2.length() ? text1 : text2;
        const string_t& shorttext = text1.length() > text2.length() ? text2 : text1;
        const size_t i = longtext.find(shorttext);
        if (i != string_t::npos) {
            const operation_t op = (text1.length() > text2.length()) ? operation_t::remove : operation_t::insert;
            diffs.push_back(diff_t(op, longtext.substr(0, i)));
            diffs.push_back(diff_t(operation_t::equal, shorttext));
            diffs.push_back(diff_t(op, safe_mid(longtext, i + shorttext.length())));
            return;
        }
        if (shorttext.length() == 1) {
            diffs.push_back(diff_t(operation_t::remove, text1));
            diffs.push_back(diff_t(operation_t::insert, text2));
            return;
        }
        if (deadline != std::numeric_limits<clock_t>::max()) {
            half_match_result_t hm;
            if (diff_half_match(text1, text2, hm)) {
                diff_main(hm.text1_a, hm.text2_a, checklines, deadline, diffs);
                diffs.push_back(diff_t(operation_t::equal, hm.mid_common));
                list_diff_t diffs_b;
                diff_main(hm.text1_b, hm.text2_b, checklines, deadline, diffs_b);
                diffs.splice(diffs.end(), diffs_b);
                return;
            }
        }
        if (checklines && text1.length() > 100 && text2.length() > 100) {
            diff_line_mode(text1, text2, deadline, diffs);
            return;
        }
        diff_bisect(text1, text2, deadline, diffs);
    }

    static void diff_line_mode(string_t text1, string_t text2, clock_t deadline, list_diff_t& diffs) {
        vector_line_t linearray;
        diff_lines_to_chars(text1, text2, linearray);

        diff_main(text1, text2, false, deadline, diffs);

        diff_chars_to_lines(diffs, linearray);
        diff_cleanup_semantic(diffs);

        diffs.push_back(diff_t(operation_t::equal, string_t()));
        ssize_t count_delete = 0;
        ssize_t count_insert = 0;
        string_t text_delete;
        string_t text_insert;

        for (typename list_diff_t::iterator cur_diff = diffs.begin(); cur_diff != diffs.end(); ++cur_diff) {
            switch ((*cur_diff).operation) {
            case operation_t::insert:
                count_insert++;
                text_insert += (*cur_diff).text;
                break;
            case operation_t::remove:
                count_delete++;
                text_delete += (*cur_diff).text;
                break;
            case operation_t::equal:
                if (count_delete >= 1 && count_insert >= 1) {
                    typename list_diff_t::iterator last = cur_diff;
                    std::advance(cur_diff, -(count_delete + count_insert));
                    cur_diff = diffs.erase(cur_diff, last);

                    list_diff_t new_diffs;
                    diff_main(text_delete, text_insert, false, deadline, new_diffs);
                    diffs.splice(cur_diff++, new_diffs);
                    --cur_diff;
                }
                count_insert = 0;
                count_delete = 0;
                text_delete.clear();
                text_insert.clear();
                break;
            }
        }
        diffs.pop_back();
    }

protected:
    static list_diff_t diff_bisect(const string_t &text1, const string_t &text2, clock_t deadline) {
        list_diff_t diffs;
        diff_bisect(text1, text2, deadline, diffs);
        return diffs;
    }

private:
    static void diff_bisect(const string_t &text1, const string_t &text2, clock_t deadline, list_diff_t& diffs) {
        const ssize_t text1_length = text1.length();
        const ssize_t text2_length = text2.length();
        const ssize_t max_d = (text1_length + text2_length + 1) / 2;
        const ssize_t v_offset = max_d;
        const ssize_t v_length = 2 * max_d;
        std::vector<ssize_t> v1(v_length, -1), v2(v_length, -1);
        v1[v_offset + 1] = 0;
        v2[v_offset + 1] = 0;
        const ssize_t delta = text1_length - text2_length;
        const bool front = (delta % 2 != 0);
        ssize_t k1start = 0;
        ssize_t k1end = 0;
        ssize_t k2start = 0;
        ssize_t k2end = 0;
        for (ssize_t d = 0; d < max_d; d++) {
            if (clock() > deadline) {
                break;
            }

            for (ssize_t k1 = -d + k1start; k1 <= d - k1end; k1 += 2) {
                const ssize_t k1_offset = v_offset + k1;
                ssize_t x1;
                if (k1 == -d || (k1 != d && v1[k1_offset - 1] < v1[k1_offset + 1])) {
                    x1 = v1[k1_offset + 1];
                } else {
                    x1 = v1[k1_offset - 1] + 1;
                }
                ssize_t y1 = x1 - k1;
                while (x1 < text1_length && y1 < text2_length
                       && text1[x1] == text2[y1]) {
                    x1++;
                    y1++;
                }
                v1[k1_offset] = x1;
                if (x1 > text1_length) {
                    k1end += 2;
                } else if (y1 > text2_length) {
                    k1start += 2;
                } else if (front) {
                    ssize_t k2_offset = v_offset + delta - k1;
                    if (k2_offset >= 0 && k2_offset < v_length && v2[k2_offset] != -1) {
                        ssize_t x2 = text1_length - v2[k2_offset];
                        if (x1 >= x2) {
                            diff_bisect_split(text1, text2, x1, y1, deadline, diffs);
                            return;
                        }
                    }
                }
            }

            for (ssize_t k2 = -d + k2start; k2 <= d - k2end; k2 += 2) {
                const ssize_t k2_offset = v_offset + k2;
                ssize_t x2;
                if (k2 == -d || (k2 != d && v2[k2_offset - 1] < v2[k2_offset + 1])) {
                    x2 = v2[k2_offset + 1];
                } else {
                    x2 = v2[k2_offset - 1] + 1;
                }
                ssize_t y2 = x2 - k2;
                while (x2 < text1_length && y2 < text2_length
                       && text1[text1_length - x2 - 1] == text2[text2_length - y2 - 1]) {
                    x2++;
                    y2++;
                }
                v2[k2_offset] = x2;
                if (x2 > text1_length) {
                    k2end += 2;
                } else if (y2 > text2_length) {
                    k2start += 2;
                } else if (!front) {
                    ssize_t k1_offset = v_offset + delta - k2;
                    if (k1_offset >= 0 && k1_offset < v_length && v1[k1_offset] != -1) {
                        ssize_t x1 = v1[k1_offset];
                        ssize_t y1 = v_offset + x1 - k1_offset;
                        x2 = text1_length - x2;
                        if (x1 >= x2) {
                            diff_bisect_split(text1, text2, x1, y1, deadline, diffs);
                            return;
                        }
                    }
                }
            }
        }
        diffs.clear();
        diffs.push_back(diff_t(operation_t::remove, text1));
        diffs.push_back(diff_t(operation_t::insert, text2));
    }

private:
    static void diff_bisect_split(const string_t &text1, const string_t &text2, ssize_t x, ssize_t y, clock_t deadline, list_diff_t& diffs) {
        string_t text1a = text1.substr(0, x);
        string_t text2a = text2.substr(0, y);
        string_t text1b = safe_mid(text1, x);
        string_t text2b = safe_mid(text2, y);

        diff_main(text1a, text2a, false, deadline, diffs);
        list_diff_t diffs_b;
        diff_main(text1b, text2b, false, deadline, diffs_b);
        diffs.splice(diffs.end(), diffs_b);
    }

protected:
    struct ptr_line_t : std::pair<typename string_t::const_pointer, size_t> {
        ptr_line_t() = default;
        ptr_line_t(typename string_t::const_pointer p, size_t n) : std::pair<typename string_t::const_pointer, size_t>(p, n) {}
        bool operator<(const ptr_line_t& p) const {
            return this->second < p.second
                    ? true
                    : this->second > p.second
                      ? false
                      : string_t::traits_type::compare(this->first, p.first, this->second) < 0;
        }
    };

    struct vector_line_t : std::vector<ptr_line_t> { string_t text1, text2; };

    static void diff_lines_to_chars(string_t &text1, string_t &text2, vector_line_t& linearray) {
        std::map<ptr_line_t, size_t> linehash;
        linearray.text1.swap(text1), linearray.text2.swap(text2);

        text1 = diff_lines_to_chars_munge(linearray.text1, linehash);
        text2 = diff_lines_to_chars_munge(linearray.text2, linehash);

        linearray.resize(linehash.size() + 1);
        for (typename std::map<ptr_line_t, size_t>::const_iterator i = linehash.begin(); i != linehash.end(); ++i)
            linearray[(*i).second] = (*i).first;
    }

private:
    static string_t diff_lines_to_chars_munge(const string_t &text, std::map<ptr_line_t, size_t> &linehash) {
        string_t chars;
        typename string_t::size_type line_len;
        for (typename string_t::const_pointer line_start = text.c_str(), text_end = line_start + text.size(); line_start < text_end; line_start += line_len + 1) {
            line_len = next_token(text, traits::from_wchar(L'\n'), line_start);
            if (line_start + line_len == text_end) --line_len;
            chars += (char_t)(*linehash.insert(std::make_pair(ptr_line_t(line_start, line_len + 1), linehash.size() + 1)).first).second;
        }
        return chars;
    }

private:
    static void diff_chars_to_lines(list_diff_t &diffs, const vector_line_t& linearray) {
        for (typename list_diff_t::iterator cur_diff = diffs.begin(); cur_diff != diffs.end(); ++cur_diff) {
            string_t text;
            for (ssize_t y = 0; y < (ssize_t)(*cur_diff).text.length(); y++) {
                const ptr_line_t& lp = linearray[static_cast<size_t>((*cur_diff).text[y])];
                text.append(lp.first, lp.second);
            }
            (*cur_diff).text.swap(text);
        }
    }

public:
    static ssize_t diff_common_prefix(const string_t &text1, const string_t &text2) {
        const ssize_t n = std::min(text1.length(), text2.length());
        for (ssize_t i = 0; i < n; i++) {
            if (text1[i] != text2[i]) {
                return i;
            }
        }
        return n;
    }

public:
    static ssize_t diff_common_suffix(const string_t &text1, const string_t &text2) {
        const ssize_t text1_length = text1.length();
        const ssize_t text2_length = text2.length();
        const ssize_t n = std::min(text1_length, text2_length);
        for (ssize_t i = 1; i <= n; i++) {
            if (text1[text1_length - i] != text2[text2_length - i]) {
                return i - 1;
            }
        }
        return n;
    }

protected:
    static ssize_t diff_common_overlap(const string_t &text1, const string_t &text2) {
        const ssize_t text1_length = text1.length();
        const ssize_t text2_length = text2.length();
        if (text1_length == 0 || text2_length == 0) {
            return 0;
        }
        string_t text1_trunc = text1;
        string_t text2_trunc = text2;
        if (text1_length > text2_length) {
            text1_trunc = right(text1, text2_length);
        } else if (text1_length < text2_length) {
            text2_trunc = text2.substr(0, text1_length);
        }
        const ssize_t text_length = std::min(text1_length, text2_length);
        if (text1_trunc == text2_trunc) {
            return text_length;
        }

        ssize_t best = 0;
        ssize_t length = 1;
        while (true) {
            string_t pattern = right(text1_trunc, length);
            size_t found = text2_trunc.find(pattern);
            if (found == string_t::npos) {
                return best;
            }
            length += found;
            if (found == 0 || right(text1_trunc, length) == text2_trunc.substr(0, length)) {
                best = length;
                length++;
            }
        }
    }

protected:
    struct half_match_result_t {
        string_t text1_a, text1_b, text2_a, text2_b, mid_common;
        void swap(half_match_result_t& hm) {
            text1_a.swap(hm.text1_a), text1_b.swap(hm.text1_b), text2_a.swap(hm.text2_a), text2_b.swap(hm.text2_b), mid_common.swap(hm.mid_common);
        }
    };

    static bool diff_half_match(const string_t &text1, const string_t &text2, half_match_result_t& hm) {
        const string_t longtext = text1.length() > text2.length() ? text1 : text2;
        const string_t shorttext = text1.length() > text2.length() ? text2 : text1;
        if (longtext.length() < 4 || shorttext.length() * 2 < longtext.length()) {
            return false;
        }

        half_match_result_t res1, res2;
        bool hm1 = diff_half_match_i(longtext, shorttext, (longtext.length() + 3) / 4, res1);
        bool hm2 = diff_half_match_i(longtext, shorttext, (longtext.length() + 1) / 2, res2);
        if (!hm1 && !hm2) {
            return false;
        } else if (!hm2) {
            hm.swap(res1);
        } else if (!hm1) {
            hm.swap(res2);
        } else {
            hm.swap(res1.mid_common.length() > res2.mid_common.length() ? res1 : res2);
        }

        if (text1.length() <= text2.length()) {
            hm.text1_a.swap(hm.text2_a);
            hm.text1_b.swap(hm.text2_b);
        }
        return true;
    }

private:
    static bool diff_half_match_i(const string_t &longtext, const string_t &shorttext, ssize_t i, half_match_result_t& best) {
        const string_t seed = safe_mid(longtext, i, longtext.length() / 4);
        size_t j = string_t::npos;
        while ((j = shorttext.find(seed, j + 1)) != string_t::npos) {
            const ssize_t prefix_len = diff_common_prefix(safe_mid(longtext, i), safe_mid(shorttext, j));
            const ssize_t suffix_len = diff_common_suffix(longtext.substr(0, i), shorttext.substr(0, j));
            if ((ssize_t)best.mid_common.length() < suffix_len + prefix_len) {
                best.mid_common = safe_mid(shorttext, j - suffix_len, suffix_len) + safe_mid(shorttext, j, prefix_len);
                best.text1_a = longtext.substr(0, i - suffix_len);
                best.text1_b = safe_mid(longtext, i + prefix_len);
                best.text2_a = shorttext.substr(0, j - suffix_len);
                best.text2_b = safe_mid(shorttext, j + prefix_len);
            }
        }
        return best.mid_common.length() * 2 >= longtext.length();
    }

public:
    static void diff_cleanup_semantic(list_diff_t &diffs) {
        if (diffs.empty()) {
            return;
        }
        bool changes = false;
        std::vector<typename list_diff_t::iterator> equalities;
        string_t lastequality;
        typename list_diff_t::iterator cur_diff;
        ssize_t length_insertions1 = 0;
        ssize_t length_deletions1 = 0;
        ssize_t length_insertions2 = 0;
        ssize_t length_deletions2 = 0;
        for (cur_diff = diffs.begin(); cur_diff != diffs.end();) {
            if ((*cur_diff).operation == operation_t::equal) {
                equalities.push_back(cur_diff);
                length_insertions1 = length_insertions2;
                length_deletions1 = length_deletions2;
                length_insertions2 = 0;
                length_deletions2 = 0;
                lastequality = (*cur_diff).text;
            } else {
                if ((*cur_diff).operation == operation_t::insert) {
                    length_insertions2 += (*cur_diff).text.length();
                } else {
                    length_deletions2 += (*cur_diff).text.length();
                }
                if (!lastequality.empty()
                        && ((ssize_t)lastequality.length()
                            <= std::max(length_insertions1, length_deletions1))
                        && ((ssize_t)lastequality.length()
                            <= std::max(length_insertions2, length_deletions2))) {
                    (*(cur_diff = equalities.back())).operation = operation_t::insert;
                    diffs.insert(cur_diff, diff_t(operation_t::remove, lastequality));
                    equalities.pop_back();
                    if (!equalities.empty()) {
                        equalities.pop_back();
                    }
                    length_insertions1 = 0;
                    length_deletions1 = 0;
                    length_insertions2 = 0;
                    length_deletions2 = 0;
                    lastequality = string_t();
                    changes = true;

                    if (!equalities.empty())
                        cur_diff = equalities.back();
                    else
                    {
                        cur_diff = diffs.begin();
                        continue;
                    }
                }
            }
            ++cur_diff;
        }

        if (changes) {
            diff_cleanup_merge(diffs);
        }
        diff_cleanup_semantic_lossless(diffs);

        if ((cur_diff = diffs.begin()) != diffs.end()) {
            for (typename list_diff_t::iterator prev_diff = cur_diff; ++cur_diff != diffs.end(); prev_diff = cur_diff) {
                if ((*prev_diff).operation == operation_t::remove && (*cur_diff).operation == operation_t::insert) {
                    string_t deletion = (*prev_diff).text;
                    string_t insertion = (*cur_diff).text;
                    ssize_t overlap_length1 = diff_common_overlap(deletion, insertion);
                    ssize_t overlap_length2 = diff_common_overlap(insertion, deletion);
                    if (overlap_length1 >= overlap_length2) {
                        if (overlap_length1 >= deletion.size() / 2.0 || overlap_length1 >= insertion.size() / 2.0) {
                            diffs.insert(cur_diff, diff_t(operation_t::equal, insertion.substr(0, overlap_length1)));
                            prev_diff->text = deletion.substr(0, deletion.length() - overlap_length1);
                            cur_diff->text = safe_mid(insertion, overlap_length1);
                        }
                    } else {
                        if (overlap_length2 >= deletion.length() / 2.0 || overlap_length2 >= insertion.length() / 2.0) {
                            diffs.insert(cur_diff, diff_t(operation_t::equal, deletion.substr(0, overlap_length2)));
                            prev_diff->operation = operation_t::insert;
                            prev_diff->text = insertion.substr(0, insertion.length() - overlap_length2);
                            cur_diff->operation = operation_t::remove;
                            cur_diff->text = safe_mid(deletion, overlap_length2);
                        }
                    }
                    if (++cur_diff == diffs.end()) break;
                }
            }
        }
    }

public:
    static void diff_cleanup_semantic_lossless(list_diff_t &diffs) {
        string_t equality1, edit, equality2;
        string_t common_string;
        ssize_t common_offset;
        ssize_t score, best_score;
        string_t best_equality1, best_edit, best_equality2;
        typename list_diff_t::iterator prev_diff = diffs.begin(), cur_diff = prev_diff;
        if (prev_diff == diffs.end() || ++cur_diff == diffs.end()) return;

        for (typename list_diff_t::iterator next_diff = cur_diff; ++next_diff != diffs.end(); prev_diff = cur_diff, cur_diff = next_diff) {
            if ((*prev_diff).operation == operation_t::equal && (*next_diff).operation == operation_t::equal) {
                equality1 = (*prev_diff).text;
                edit = (*cur_diff).text;
                equality2 = (*next_diff).text;

                common_offset = diff_common_suffix(equality1, edit);
                if (common_offset != 0) {
                    common_string = safe_mid(edit, edit.length() - common_offset);
                    equality1 = equality1.substr(0, equality1.length() - common_offset);
                    edit = common_string + edit.substr(0, edit.length() - common_offset);
                    equality2 = common_string + equality2;
                }

                best_equality1 = equality1;
                best_edit = edit;
                best_equality2 = equality2;
                best_score = diff_cleanup_semantic_score(equality1, edit) + diff_cleanup_semantic_score(edit, equality2);
                while (!edit.empty() && !equality2.empty() && edit[0] == equality2[0]) {
                    equality1 += edit[0];
                    edit = safe_mid(edit, 1) + equality2[0];
                    equality2 = safe_mid(equality2, 1);
                    score = diff_cleanup_semantic_score(equality1, edit) + diff_cleanup_semantic_score(edit, equality2);
                    if (score >= best_score) {
                        best_score = score;
                        best_equality1 = equality1;
                        best_edit = edit;
                        best_equality2 = equality2;
                    }
                }

                if ((*prev_diff).text != best_equality1) {
                    if (!best_equality1.empty()) {
                        (*prev_diff).text = best_equality1;
                    } else {
                        diffs.erase(prev_diff);
                    }
                    (*cur_diff).text = best_edit;
                    if (!best_equality2.empty()) {
                        (*next_diff).text = best_equality2;
                    } else {
                        diffs.erase(next_diff);
                        next_diff = cur_diff;
                        cur_diff = prev_diff;
                    }
                }
            }
        }
    }

private:
    static ssize_t diff_cleanup_semantic_score(const string_t &one, const string_t &two) {
        if (one.empty() || two.empty()) {
            return 6;
        }

        char_t char1 = one[one.length() - 1];
        char_t char2 = two[0];
        bool non_alpha_numeric1 = !traits::is_alnum(char1);
        bool non_alpha_numeric2 = !traits::is_alnum(char2);
        bool whitespace1 = non_alpha_numeric1 && traits::is_space(char1);
        bool whitespace2 = non_alpha_numeric2 && traits::is_space(char2);
        bool line_break1 = whitespace1 && is_control(char1);
        bool line_break2 = whitespace2 && is_control(char2);
        bool blank_line1 = false;
        if (line_break1) {
            typename string_t::const_reverse_iterator p1 = one.rbegin(), p2 = one.rend();
            if (traits::to_wchar(*p1) == L'\n' && ++p1 != p2) {
                if (traits::to_wchar(*p1) == L'\r')
                    ++p1;
                blank_line1 = p1 != p2 && traits::to_wchar(*p1) == L'\n';
            }
        }
        bool blank_line2 = false;
        if (line_break2) {
            typename string_t::const_iterator p1 = two.end(), p2 = two.begin();
            if (traits::to_wchar(*p2) == L'\r')
                ++p2;
            if (p2 != p1 && traits::to_wchar(*p2) == L'\n') {
                if (++p2 != p1 && traits::to_wchar(*p2) == L'\r')
                    ++p2;
                if (p2 != p1 && traits::to_wchar(*p2) == L'\n')
                    blank_line2 = true;
            }
        }

        if (blank_line1 || blank_line2) {
            return 5;
        } else if (line_break1 || line_break2) {
            return 4;
        } else if (non_alpha_numeric1 && !whitespace1 && whitespace2) {
            return 3;
        } else if (whitespace1 || whitespace2) {
            return 2;
        } else if (non_alpha_numeric1 || non_alpha_numeric2) {
            return 1;
        }
        return 0;
    }

public:
    void diff_cleanup_efficiency(list_diff_t &diffs) const {
        if (diffs.empty()) {
            return;
        }
        bool changes = false;
        std::vector<typename list_diff_t::iterator> equalities;
        string_t lastequality;
        bool pre_ins = false;
        bool pre_del = false;
        bool post_ins = false;
        bool post_del = false;

        for (typename list_diff_t::iterator cur_diff = diffs.begin(); cur_diff != diffs.end();) {
            if ((*cur_diff).operation == operation_t::equal) {
                if ((ssize_t)(*cur_diff).text.length() < diff_edit_cost && (post_ins || post_del)) {
                    equalities.push_back(cur_diff);
                    pre_ins = post_ins;
                    pre_del = post_del;
                    lastequality = (*cur_diff).text;
                } else {
                    equalities.clear();
                    lastequality.clear();
                }
                post_ins = post_del = false;
            } else {
                if ((*cur_diff).operation == operation_t::remove) {
                    post_del = true;
                } else {
                    post_ins = true;
                }
                if (!lastequality.empty()
                        && ((pre_ins && pre_del && post_ins && post_del)
                            || (((ssize_t)lastequality.length() < diff_edit_cost / 2)
                                && ((pre_ins ? 1 : 0) + (pre_del ? 1 : 0)
                                    + (post_ins ? 1 : 0) + (post_del ? 1 : 0)) == 3))) {
                    (*(cur_diff = equalities.back())).operation = operation_t::insert;
                    diffs.insert(cur_diff, diff_t(operation_t::remove, lastequality));
                    equalities.pop_back();
                    lastequality.clear();
                    changes = true;
                    if (pre_ins && pre_del) {
                        post_ins = post_del = true;
                        equalities.clear();
                    } else {
                        if (!equalities.empty()) {
                            equalities.pop_back();
                        }
                        post_ins = post_del = false;
                        if (!equalities.empty())
                            cur_diff = equalities.back();
                        else
                        {
                            cur_diff = diffs.begin();
                            continue;
                        }
                    }
                }
            }
            ++cur_diff;
        }

        if (changes) {
            diff_cleanup_merge(diffs);
        }
    }

public:
    static void diff_cleanup_merge(list_diff_t &diffs) {
        diffs.push_back(diff_t(operation_t::equal, string_t()));
        typename list_diff_t::iterator prev_diff, cur_diff;
        ssize_t count_delete = 0;
        ssize_t count_insert = 0;
        string_t text_delete;
        string_t text_insert;
        diff_t *prev_equal = NULL;
        ssize_t common_len;
        for (cur_diff = diffs.begin(); cur_diff != diffs.end(); ++cur_diff) {
            switch ((*cur_diff).operation) {
            case operation_t::insert:
                count_insert++;
                text_insert += (*cur_diff).text;
                prev_equal = NULL;
                break;
            case operation_t::remove:
                count_delete++;
                text_delete += (*cur_diff).text;
                prev_equal = NULL;
                break;
            case operation_t::equal:
                if (count_delete + count_insert > 1) {
                    prev_diff = cur_diff;
                    std::advance(prev_diff, -(count_delete + count_insert));
                    diffs.erase(prev_diff, cur_diff);
                    if (count_delete != 0 && count_insert != 0) {
                        common_len = diff_common_prefix(text_insert, text_delete);
                        if (common_len != 0) {
                            if (cur_diff != diffs.begin()) {
                                prev_diff = cur_diff;
                                if ((*--prev_diff).operation != operation_t::equal) {
                                    throw string_t(traits::cs(L"Previous diff should have been an equality."));
                                }
                                (*prev_diff).text += text_insert.substr(0, common_len);
                            } else {
                                diffs.insert(cur_diff, diff_t(operation_t::equal, text_insert.substr(0, common_len)));
                            }
                            text_insert = safe_mid(text_insert, common_len);
                            text_delete = safe_mid(text_delete, common_len);
                        }
                        common_len = diff_common_suffix(text_insert, text_delete);
                        if (common_len != 0) {
                            (*cur_diff).text = safe_mid(text_insert, text_insert.length()
                                                       - common_len) + (*cur_diff).text;
                            text_insert = text_insert.substr(0, text_insert.length()
                                                             - common_len);
                            text_delete = text_delete.substr(0, text_delete.length()
                                                             - common_len);
                        }
                    }
                    if (!text_delete.empty()) {
                        diffs.insert(cur_diff, diff_t(operation_t::remove, text_delete));
                    }
                    if (!text_insert.empty()) {
                        diffs.insert(cur_diff, diff_t(operation_t::insert, text_insert));
                    }
                } else if (prev_equal != NULL) {
                    prev_equal->text += (*cur_diff).text;
                    diffs.erase(cur_diff--);
                }

                count_insert = 0;
                count_delete = 0;
                text_delete.clear();
                text_insert.clear();
                prev_equal = &*cur_diff;
                break;
            }

        }
        if (diffs.back().text.empty()) {
            diffs.pop_back();
        }
        bool changes = false;
        prev_diff = cur_diff = diffs.begin();
        if (prev_diff != diffs.end() && ++cur_diff != diffs.end()) {
            for (typename list_diff_t::iterator next_diff = cur_diff; ++next_diff != diffs.end(); prev_diff = cur_diff, cur_diff = next_diff) {
                if ((*prev_diff).operation == operation_t::equal &&
                        (*next_diff).operation == operation_t::equal) {
                    if ((*cur_diff).text.size() >= (*prev_diff).text.size() &&
                            (*cur_diff).text.compare((*cur_diff).text.size() - (*prev_diff).text.size(), (*prev_diff).text.size(), (*prev_diff).text) == 0) {
                        (*cur_diff).text = (*prev_diff).text
                                + (*cur_diff).text.substr(0, (*cur_diff).text.length()
                                                          - (*prev_diff).text.length());
                        (*next_diff).text = (*prev_diff).text + (*next_diff).text;
                        diffs.erase(prev_diff);
                        cur_diff = next_diff;
                        changes = true;
                        if (++next_diff == diffs.end()) break;
                    } else if ((*cur_diff).text.size() >= (*next_diff).text.size() && (*cur_diff).text.compare(0, (*next_diff).text.size(), (*next_diff).text) == 0) {
                        (*prev_diff).text += (*next_diff).text;
                        (*cur_diff).text = safe_mid((*cur_diff).text, (*next_diff).text.length())
                                + (*next_diff).text;
                        next_diff = diffs.erase(next_diff);
                        changes = true;
                        if (next_diff == diffs.end()) break;
                    }
                }
            }
        }
        if (changes) {
            diff_cleanup_merge(diffs);
        }
    }

public:
    static ssize_t diff_xindex(const list_diff_t &diffs, ssize_t loc) {
        ssize_t chars1 = 0;
        ssize_t chars2 = 0;
        ssize_t last_chars1 = 0;
        ssize_t last_chars2 = 0;
        typename list_diff_t::const_iterator last_diff = diffs.end(), cur_diff;
        for (cur_diff = diffs.begin(); cur_diff != diffs.end(); ++cur_diff) {
            if ((*cur_diff).operation != operation_t::insert) {
                chars1 += (*cur_diff).text.length();
            }
            if ((*cur_diff).operation != operation_t::remove) {
                chars2 += (*cur_diff).text.length();
            }
            if (chars1 > loc) {
                last_diff = cur_diff;
                break;
            }
            last_chars1 = chars1;
            last_chars2 = chars2;
        }
        if (last_diff != diffs.end() && (*last_diff).operation == operation_t::remove) {
            return last_chars2;
        }
        return last_chars2 + (loc - last_chars1);
    }

public:
    static string_t diff_pretty_html(const list_diff_t &diffs) {
        string_t html;
        string_t text;
        for (typename list_diff_t::const_iterator cur_diff = diffs.begin(); cur_diff != diffs.end(); ++cur_diff) {
            typename string_t::size_type n = (*cur_diff).text.size();
            typename string_t::const_pointer p, end;
            for (p = (*cur_diff).text.c_str(), end = p + n; p != end; ++p)
                switch (traits::to_wchar(*p)) {
                case L'&': n += 4; break;
                case L'<':
                case L'>': n += 3; break;
                case L'\n': n += 9; break;
                }
            if (n == (*cur_diff).text.size())
                text = (*cur_diff).text;
            else {
                text.clear();
                text.reserve(n);
                for (p = (*cur_diff).text.c_str(); p != end; ++p)
                    switch (traits::to_wchar(*p)) {
                    case L'&': text += traits::cs(L"&amp;"); break;
                    case L'<': text += traits::cs(L"&lt;"); break;
                    case L'>': text += traits::cs(L"&gt;"); break;
                    case L'\n': text += traits::cs(L"&para;<br>"); break;
                    default: text += *p;
                    }
            }
            switch ((*cur_diff).operation) {
            case operation_t::insert:
                html += traits::cs(L"<ins style=\"background:#e6ffe6;\">") + text + traits::cs(L"</ins>");
                break;
            case operation_t::remove:
                html += traits::cs(L"<del style=\"background:#ffe6e6;\">") + text + traits::cs(L"</del>");
                break;
            case operation_t::equal:
                html += traits::cs(L"<span>") + text + traits::cs(L"</span>");
                break;
            }
        }
        return html;
    }

public:
    static string_t diff_text1(const list_diff_t &diffs) {
        string_t text;
        for (typename list_diff_t::const_iterator cur_diff = diffs.begin(); cur_diff != diffs.end(); ++cur_diff) {
            if ((*cur_diff).operation != operation_t::insert) {
                text += (*cur_diff).text;
            }
        }
        return text;
    }

public:
    static string_t diff_text2(const list_diff_t &diffs) {
        string_t text;
        for (typename list_diff_t::const_iterator cur_diff = diffs.begin(); cur_diff != diffs.end(); ++cur_diff) {
            if ((*cur_diff).operation != operation_t::remove) {
                text += (*cur_diff).text;
            }
        }
        return text;
    }

public:
    static ssize_t diff_levenshtein(const list_diff_t &diffs) {
        ssize_t levenshtein = 0;
        ssize_t insertions = 0;
        ssize_t deletions = 0;
        for (typename list_diff_t::const_iterator cur_diff = diffs.begin(); cur_diff != diffs.end(); ++cur_diff) {
            switch ((*cur_diff).operation) {
            case operation_t::insert:
                insertions += (*cur_diff).text.length();
                break;
            case operation_t::remove:
                deletions += (*cur_diff).text.length();
                break;
            case operation_t::equal:
                levenshtein += std::max(insertions, deletions);
                insertions = 0;
                deletions = 0;
                break;
            }
        }
        levenshtein += std::max(insertions, deletions);
        return levenshtein;
    }

public:
    static string_t diff_to_delta(const list_diff_t &diffs) {
        string_t text;
        for (typename list_diff_t::const_iterator cur_diff = diffs.begin(); cur_diff != diffs.end(); ++cur_diff) {
            switch ((*cur_diff).operation) {
            case operation_t::insert: {
                text += traits::from_wchar(L'+');
                append_percent_encoded(text, (*cur_diff).text);
                text += traits::from_wchar(L'\t');
                break;
            }
            case operation_t::remove:
                text += traits::from_wchar(L'-') + as_string((*cur_diff).text.length()) + traits::from_wchar(L'\t');
                break;
            case operation_t::equal:
                text += traits::from_wchar(L'=') + as_string((*cur_diff).text.length()) + traits::from_wchar(L'\t');
                break;
            }
        }
        if (!text.empty()) {
            text = text.substr(0, text.length() - 1);
        }
        return text;
    }

public:
    static list_diff_t diff_from_delta(const string_t &text1, const string_t &delta) {
        list_diff_t diffs;
        ssize_t pointer = 0;
        typename string_t::size_type token_len;
        for (typename string_t::const_pointer token = delta.c_str(); token - delta.c_str() < (ssize_t)delta.length(); token += token_len + 1) {
            token_len = next_token(delta, traits::from_wchar(L'\t'), token);
            if (token_len == 0) {
                continue;
            }
            string_t param(token + 1, token_len - 1);
            switch (traits::to_wchar(*token)) {
            case L'+':
                percent_decode(param);
                diffs.push_back(diff_t(operation_t::insert, param));
                break;
            case L'-':
            case L'=': {
                ssize_t n;
                n = as_int(param);
                if (n < 0) {
                    throw string_t(traits::cs(L"Negative number in diff_fromDelta: ") + param);
                }
                string_t text;
                text = safe_mid(text1, pointer, n);
                pointer += n;
                if (traits::to_wchar(*token) == L'=') {
                    diffs.push_back(diff_t(operation_t::equal, text));
                } else {
                    diffs.push_back(diff_t(operation_t::remove, text));
                }
                break;
            }
            default:
                throw string_t(string_t(traits::cs(L"Invalid diff operation in diff_fromDelta: ")) + *token);
            }
        }
        if (pointer != text1.length()) {
            throw string_t(traits::cs(L"Delta length (") + as_string(pointer)
                           + traits::cs(L") smaller than source text length (")
                           + as_string(text1.length()) + traits::from_wchar(L')'));
        }
        return diffs;
    }

public:
    ssize_t match_main(const string_t &text, const string_t &pattern, ssize_t loc) const {
        loc = std::max((ssize_t)0l, std::min(loc, (ssize_t)text.length()));
        if (text == pattern) {
            return 0;
        } else if (text.empty()) {
            return -1;
        } else if (loc + pattern.length() <= text.length() && safe_mid(text, loc, pattern.length()) == pattern) {
            return loc;
        } else {
            return match_bitap(text, pattern, loc);
        }
    }

protected:
    ssize_t match_bitap(const string_t &text, const string_t &pattern, ssize_t loc) const {
        if (!(match_max_bits == 0 || (ssize_t)pattern.length() <= match_max_bits)) {
            throw string_t(traits::cs(L"Pattern too long for this application."));
        }

        std::map<char_t, ssize_t> s;
        match_alphabet(pattern, s);

        double score_threshold = match_threshold;
        size_t best_loc = text.find(pattern, loc);
        if (best_loc != string_t::npos) {
            score_threshold = std::min(match_bitap_score(0, best_loc, loc, pattern), score_threshold);
            best_loc = text.rfind(pattern, loc + pattern.length());
            if (best_loc != string_t::npos) {
                score_threshold = std::min(match_bitap_score(0, best_loc, loc, pattern), score_threshold);
            }
        }

        ssize_t matchmask = 1 << (pattern.length() - 1);
        best_loc = -1;

        ssize_t bin_min, bin_mid;
        ssize_t bin_max = pattern.length() + text.length();
        ssize_t *rd = NULL;
        ssize_t *last_rd = NULL;
        for (ssize_t d = 0; d < (ssize_t)pattern.length(); d++) {
            bin_min = 0;
            bin_mid = bin_max;
            while (bin_min < bin_mid) {
                if (match_bitap_score(d, loc + bin_mid, loc, pattern)
                        <= score_threshold) {
                    bin_min = bin_mid;
                } else {
                    bin_max = bin_mid;
                }
                bin_mid = (bin_max - bin_min) / 2 + bin_min;
            }
            bin_max = bin_mid;
            ssize_t start = std::max((ssize_t)1l, loc - bin_mid + 1);
            ssize_t finish = std::min(loc + bin_mid, (ssize_t)text.length()) + pattern.length();

            rd = new ssize_t[finish + 2];
            rd[finish + 1] = (1 << d) - 1;
            for (ssize_t j = finish; j >= start; j--) {
                ssize_t char_match;
                if ((ssize_t)text.length() <= j - 1) {
                    char_match = 0;
                } else {
                    char_match = s[text[j - 1]];
                }
                if (d == 0) {
                    rd[j] = ((rd[j + 1] << 1) | 1) & char_match;
                } else {
                    rd[j] = (((rd[j + 1] << 1) | 1) & char_match)
                            | (((last_rd[j + 1] | last_rd[j]) << 1) | 1)
                            | last_rd[j + 1];
                }
                if ((rd[j] & matchmask) != 0) {
                    double score = match_bitap_score(d, j - 1, loc, pattern);
                    if (score <= score_threshold) {
                        score_threshold = score;
                        best_loc = j - 1;
                        if (best_loc > loc) {
                            start = std::max((ssize_t)1l, 2 * loc - (ssize_t)best_loc);
                        } else {
                            break;
                        }
                    }
                }
            }
            if (match_bitap_score(d + 1, loc, loc, pattern) > score_threshold) {
                break;
            }
            delete [] last_rd;
            last_rd = rd;
        }
        delete [] last_rd;
        delete [] rd;
        return best_loc;
    }

private:
    double match_bitap_score(ssize_t e, ssize_t x, ssize_t loc, const string_t &pattern) const {
        const float accuracy = static_cast<float> (e) / pattern.length();
        const ssize_t proximity = (loc - x < 0)? (x - loc) : (loc - x);
        if (match_distance == 0) {
            return proximity == 0 ? accuracy : 1.0;
        }
        return accuracy + (proximity / static_cast<float> (match_distance));
    }

protected:
    static void match_alphabet(const string_t &pattern, std::map<char_t, ssize_t>& s) {
        for (size_t i = 0; i < pattern.length(); i++)
            s[pattern[i]] |= (1 << (pattern.length() - i - 1));
    }

protected:
    void patch_add_context(patch_t &patch, const string_t &text) const {
        if (text.empty()) {
            return;
        }
        string_t pattern = safe_mid(text, patch.start2, patch.length1);
        ssize_t padding = 0;

        while (text.find(pattern) != text.rfind(pattern)
               && (ssize_t)pattern.length() < match_max_bits - patch_margin - patch_margin) {
            padding += patch_margin;
            pattern = safe_mid(text, std::max(0l, (long)(patch.start2 - padding)),
                              std::min((ssize_t)text.length(), patch.start2 + patch.length1 + padding)
                              - std::max(0l, (long)(patch.start2 - padding)));
        }
        padding += patch_margin;

        string_t prefix = safe_mid(text, std::max(0l, (long)(patch.start2 - padding)),
                                  patch.start2 - std::max(0l, (long)(patch.start2 - padding)));
        if (!prefix.empty()) {
            patch.diffs.push_front(diff_t(operation_t::equal, prefix));
        }
        string_t suffix = safe_mid(text, patch.start2 + patch.length1,
                                  std::min((ssize_t)text.length(), patch.start2 + patch.length1 + padding)
                                  - (patch.start2 + patch.length1));
        if (!suffix.empty()) {
            patch.diffs.push_back(diff_t(operation_t::equal, suffix));
        }

        patch.start1 -= prefix.length();
        patch.start2 -= prefix.length();
        patch.length1 += prefix.length() + suffix.length();
        patch.length2 += prefix.length() + suffix.length();
    }

public:
    list_patch_t patch_make(const string_t &text1, const string_t &text2) const {
        list_diff_t diffs = diff_main(text1, text2, true);
        if (diffs.size() > 2) {
            diff_cleanup_semantic(diffs);
            diff_cleanup_efficiency(diffs);
        }

        return patch_make(text1, diffs);
    }

public:
    list_patch_t patch_make(const list_diff_t &diffs) const {
        return patch_make(diff_text1(diffs), diffs);
    }

public:
    list_patch_t patch_make(const string_t &text1, const string_t &, const list_diff_t &diffs) const {
        return patch_make(text1, diffs);
    }

public:
    list_patch_t patch_make(const string_t &text1, const list_diff_t &diffs) const {
        list_patch_t patches;
        if (!diffs.empty()) {
            patch_t patch;
            ssize_t char_count1 = 0;
            ssize_t char_count2 = 0;
            string_t prepatch_text = text1;
            string_t postpatch_text = text1;
            for (typename list_diff_t::const_iterator cur_diff = diffs.begin(); cur_diff != diffs.end(); ++cur_diff) {
                if (patch.diffs.empty() && (*cur_diff).operation != operation_t::equal) {
                    patch.start1 = char_count1;
                    patch.start2 = char_count2;
                }

                switch ((*cur_diff).operation) {
                case operation_t::insert:
                    patch.diffs.push_back(*cur_diff);
                    patch.length2 += (*cur_diff).text.length();
                    postpatch_text = postpatch_text.substr(0, char_count2)
                            + (*cur_diff).text + safe_mid(postpatch_text, char_count2);
                    break;
                case operation_t::remove:
                    patch.length1 += (*cur_diff).text.length();
                    patch.diffs.push_back(*cur_diff);
                    postpatch_text = postpatch_text.substr(0, char_count2)
                            + safe_mid(postpatch_text, char_count2 + (*cur_diff).text.length());
                    break;
                case operation_t::equal:
                    if ((ssize_t)(*cur_diff).text.length() <= 2 * patch_margin
                            && !patch.diffs.empty() && !(*cur_diff == diffs.back())) {
                        patch.diffs.push_back(*cur_diff);
                        patch.length1 += (*cur_diff).text.length();
                        patch.length2 += (*cur_diff).text.length();
                    }

                    if ((ssize_t)(*cur_diff).text.length() >= 2 * patch_margin) {
                        if (!patch.diffs.empty()) {
                            patch_add_context(patch, prepatch_text);
                            patches.push_back(patch);
                            patch = patch_t();
                            prepatch_text = postpatch_text;
                            char_count1 = char_count2;
                        }
                    }
                    break;
                }

                if ((*cur_diff).operation != operation_t::insert) {
                    char_count1 += (*cur_diff).text.length();
                }
                if ((*cur_diff).operation != operation_t::remove) {
                    char_count2 += (*cur_diff).text.length();
                }
            }
            if (!patch.diffs.empty()) {
                patch_add_context(patch, prepatch_text);
                patches.push_back(patch);
            }
        }
        return patches;
    }

public:
    list_patch_t patch_deep_copy(const list_patch_t &patches) const { return patches; }

public:
    std::pair<string_t, std::vector<bool> > patch_apply(const list_patch_t &patches, const string_t &text) const {
        std::pair<string_t, std::vector<bool> > res;
        patch_apply(patches, text, res);
        return res;
    }

    void patch_apply(const list_patch_t &patches, const string_t &source_text, std::pair<string_t, std::vector<bool> >& res) const {
        if (patches.empty()) {
            res.first = source_text;
            res.second.clear();
            return;
        }
        string_t text = source_text;

        list_patch_t patches_copy(patches);

        string_t null_padding = patch_add_padding(patches_copy);
        text = null_padding + text + null_padding;
        patch_split_max(patches_copy);

        ssize_t x = 0;
        ssize_t delta = 0;
        std::vector<bool>& results = res.second;
        results.resize(patches_copy.size());
        string_t text1, text2;
        for (typename list_patch_t::const_iterator cur_patch = patches_copy.begin(); cur_patch != patches_copy.end(); ++cur_patch) {
            ssize_t expected_loc = (*cur_patch).start2 + delta;
            text1 = diff_text1((*cur_patch).diffs);
            ssize_t start_loc;
            ssize_t end_loc = -1;
            if ((ssize_t)text1.length() > match_max_bits) {
                start_loc = match_main(text, text1.substr(0, match_max_bits), expected_loc);
                if (start_loc != -1) {
                    end_loc = match_main(text, right(text1, match_max_bits),
                                         expected_loc + text1.length() - match_max_bits);
                    if (end_loc == -1 || start_loc >= end_loc) {
                        start_loc = -1;
                    }
                }
            } else {
                start_loc = match_main(text, text1, expected_loc);
            }
            if (start_loc == -1) {
                results[x] = false;
                delta -= (*cur_patch).length2 - (*cur_patch).length1;
            } else {
                results[x] = true;
                delta = start_loc - expected_loc;
                if (end_loc == -1) {
                    text2 = safe_mid(text, start_loc, text1.length());
                } else {
                    text2 = safe_mid(text, start_loc, end_loc + match_max_bits - start_loc);
                }
                if (text1 == text2) {
                    text = text.substr(0, start_loc) + diff_text2((*cur_patch).diffs) + safe_mid(text, start_loc + text1.length());
                } else {
                    list_diff_t diffs = diff_main(text1, text2, false);
                    if ((ssize_t)text1.length() > match_max_bits
                            && diff_levenshtein(diffs) / static_cast<float> (text1.length())
                            > patch_delete_threshold) {
                        results[x] = false;
                    } else {
                        diff_cleanup_semantic_lossless(diffs);
                        ssize_t index1 = 0;
                        for (typename list_diff_t::const_iterator cur_diff = (*cur_patch).diffs.begin(); cur_diff != (*cur_patch).diffs.end(); ++cur_diff) {
                            if ((*cur_diff).operation != operation_t::equal) {
                                ssize_t index2 = diff_xindex(diffs, index1);
                                if ((*cur_diff).operation == operation_t::insert) {
                                    text = text.substr(0, start_loc + index2) + (*cur_diff).text
                                            + safe_mid(text, start_loc + index2);
                                } else if ((*cur_diff).operation == operation_t::remove) {
                                    text = text.substr(0, start_loc + index2)
                                            + safe_mid(text, start_loc + diff_xindex(diffs,
                                                                                    index1 + (*cur_diff).text.length()));
                                }
                            }
                            if ((*cur_diff).operation != operation_t::remove) {
                                index1 += (*cur_diff).text.length();
                            }
                        }
                    }
                }
            }
            x++;
        }
        res.first = safe_mid(text, null_padding.length(), text.length() - 2 * null_padding.length());
    }

public:
    string_t patch_add_padding(list_patch_t &patches) const {
        short padding_len = patch_margin;
        string_t null_padding;
        for (short x = 1; x <= padding_len; x++) {
            null_padding += (char_t)x;
        }

        for (typename list_patch_t::iterator cur_patch = patches.begin(); cur_patch != patches.end(); ++cur_patch) {
            (*cur_patch).start1 += padding_len;
            (*cur_patch).start2 += padding_len;
        }

        patch_t &first_patch = patches.front();
        list_diff_t &first_patch_diffs = first_patch.diffs;
        if (first_patch_diffs.empty() || first_patch_diffs.front().operation != operation_t::equal) {
            first_patch_diffs.push_front(diff_t(operation_t::equal, null_padding));
            first_patch.start1 -= padding_len;
            first_patch.start2 -= padding_len;
            first_patch.length1 += padding_len;
            first_patch.length2 += padding_len;
        } else if (padding_len > (ssize_t)first_patch_diffs.front().text.length()) {
            diff_t &first_fiff = first_patch_diffs.front();
            ssize_t extra_len = padding_len - first_fiff.text.length();
            first_fiff.text = safe_mid(null_padding, first_fiff.text.length(), padding_len - first_fiff.text.length()) + first_fiff.text;
            first_patch.start1 -= extra_len;
            first_patch.start2 -= extra_len;
            first_patch.length1 += extra_len;
            first_patch.length2 += extra_len;
        }

        patch_t &last_patch = patches.front();
        list_diff_t &last_patch_diffs = last_patch.diffs;
        if (last_patch_diffs.empty() || last_patch_diffs.back().operation != operation_t::equal) {
            last_patch_diffs.push_back(diff_t(operation_t::equal, null_padding));
            last_patch.length1 += padding_len;
            last_patch.length2 += padding_len;
        } else if (padding_len > (ssize_t)last_patch_diffs.back().text.length()) {
            diff_t &last_diff = last_patch_diffs.back();
            ssize_t extra_len = padding_len - last_diff.text.length();
            last_diff.text += null_padding.substr(0, extra_len);
            last_patch.length1 += extra_len;
            last_patch.length2 += extra_len;
        }

        return null_padding;
    }

public:
    void patch_split_max(list_patch_t &patches) const {
        short patch_size = match_max_bits;
        string_t precontext, postcontext;
        patch_t patch;
        ssize_t start1, start2;
        bool empty;
        operation_t diff_type;
        string_t diff_text;
        patch_t bigpatch;

        for (typename list_patch_t::iterator cur_patch = patches.begin(); cur_patch != patches.end();) {
            if ((*cur_patch).length1 <= patch_size) { ++cur_patch; continue; }
            bigpatch = *cur_patch;
            cur_patch = patches.erase(cur_patch);
            start1 = bigpatch.start1;
            start2 = bigpatch.start2;
            precontext.clear();
            while (!bigpatch.diffs.empty()) {
                patch = patch_t();
                empty = true;
                patch.start1 = start1 - precontext.length();
                patch.start2 = start2 - precontext.length();
                if (!precontext.empty()) {
                    patch.length1 = patch.length2 = precontext.length();
                    patch.diffs.push_back(diff_t(operation_t::equal, precontext));
                }
                while (!bigpatch.diffs.empty() && patch.length1 < patch_size - patch_margin) {
                    diff_type = bigpatch.diffs.front().operation;
                    diff_text = bigpatch.diffs.front().text;
                    if (diff_type == operation_t::insert) {
                        patch.length2 += diff_text.length();
                        start2 += diff_text.length();
                        patch.diffs.push_back(bigpatch.diffs.front());
                        bigpatch.diffs.pop_front();
                        empty = false;
                    } else if (diff_type == operation_t::remove && patch.diffs.size() == 1
                               && patch.diffs.front().operation == operation_t::equal
                               && (ssize_t)diff_text.length() > 2 * patch_size) {
                        patch.length1 += diff_text.length();
                        start1 += diff_text.length();
                        empty = false;
                        patch.diffs.push_back(diff_t(diff_type, diff_text));
                        bigpatch.diffs.pop_front();
                    } else {
                        diff_text = diff_text.substr(0, std::min((ssize_t)diff_text.length(),
                                                                 patch_size - patch.length1 - patch_margin));
                        patch.length1 += diff_text.length();
                        start1 += diff_text.length();
                        if (diff_type == operation_t::equal) {
                            patch.length2 += diff_text.length();
                            start2 += diff_text.length();
                        } else {
                            empty = false;
                        }
                        patch.diffs.push_back(diff_t(diff_type, diff_text));
                        if (diff_text == bigpatch.diffs.front().text) {
                            bigpatch.diffs.pop_front();
                        } else {
                            bigpatch.diffs.front().text = safe_mid(bigpatch.diffs.front().text, diff_text.length());
                        }
                    }
                }
                precontext = safe_mid(diff_text2(patch.diffs), std::max((ssize_t)0, (ssize_t)precontext.length() - patch_margin));
                postcontext = diff_text1(bigpatch.diffs);
                if ((ssize_t)postcontext.length() > patch_margin) {
                    postcontext = postcontext.substr(0, patch_margin);
                }
                if (!postcontext.empty()) {
                    patch.length1 += postcontext.length();
                    patch.length2 += postcontext.length();
                    if (!patch.diffs.empty() && patch.diffs.back().operation == operation_t::equal) {
                        patch.diffs.back().text += postcontext;
                    } else {
                        patch.diffs.push_back(diff_t(operation_t::equal, postcontext));
                    }
                }
                if (!empty) {
                    patches.insert(cur_patch, patch);
                }
            }
        }
    }

public:
    static string_t patch_to_text(const list_patch_t &patches) {
        string_t text;
        for (typename list_patch_t::const_iterator cur_patch = patches.begin(); cur_patch != patches.end(); ++cur_patch) {
            text += (*cur_patch).to_string();
        }
        return text;
    }

public:
    list_patch_t patch_from_text(const string_t &textline) const {
        list_patch_t patches;
        if (!textline.empty()) {
            char_t sign;
            string_t line;
            typename string_t::const_pointer text = textline.c_str();
            typename string_t::size_type text_len, l;
            while (text - textline.c_str() < (ssize_t)textline.length()) {
                if ((text_len = next_token(textline, traits::from_wchar(L'\n'), text)) == 0) { ++text; continue; }
                string_t start1, length1, start2, length2;
                do {
                    typename string_t::const_pointer t = text;
                    l = text_len;
                    if ((l -= 9) > 0 && traits::to_wchar(*t) == L'@' && traits::to_wchar(*++t) == L'@'
                            && traits::to_wchar(*++t) == L' ' && traits::to_wchar(*++t) == L'-' && traits::is_digit(*++t)) {
                        do { start1 += *t; } while (--l > 0 && traits::is_digit(*++t));
                        if (l > 0 && traits::to_wchar(*t) == L',') ++t, --l;
                        while (l > 0 && traits::is_digit(*t)) --l, length1 += *t++;
                        if (l > 0 && traits::to_wchar(*t++) == L' ' && traits::to_wchar(*t++) == L'+' && traits::is_digit(*t)) {
                            do { start2 += *t; --l; } while (traits::is_digit(*++t));
                            if (l > 0 && traits::to_wchar(*t) == L',') ++t, --l;
                            while (l > 0 && traits::is_digit(*t)) --l, length2 += *t++;
                            if (l == 0 && traits::to_wchar(*t++) == L' ' && traits::to_wchar(*t++) == L'@' && traits::to_wchar(*t) == L'@') break;
                        }
                    }
                    throw string_t(traits::cs(L"Invalid patch string: ") + string_t(text, text_len));
                } while (false);

                patch_t patch;
                patch.start1 = as_int(start1);
                if (length1.empty()) {
                    patch.start1--;
                    patch.length1 = 1;
                } else if (length1.size() == 1 && traits::to_wchar(length1[0]) == L'0') {
                    patch.length1 = 0;
                } else {
                    patch.start1--;
                    patch.length1 = as_int(length1);
                }

                patch.start2 = as_int(start2);
                if (length2.empty()) {
                    patch.start2--;
                    patch.length2 = 1;
                } else if (length2.size() == 1 && traits::to_wchar(length2[0]) == L'0') {
                    patch.length2 = 0;
                } else {
                    patch.start2--;
                    patch.length2 = as_int(length2);
                }

                for (text += text_len + 1; text - textline.c_str() < (ssize_t)textline.length(); text += text_len + 1) {
                    if ((text_len = next_token(textline, traits::from_wchar(L'\n'), text)) == 0) continue;

                    sign = *text;
                    line.assign(text + 1, text_len - 1);
                    percent_decode(line);
                    switch (traits::to_wchar(sign)) {
                    case L'-':
                        patch.diffs.push_back(diff_t(operation_t::remove, line));
                        continue;
                    case L'+':
                        patch.diffs.push_back(diff_t(operation_t::insert, line));
                        continue;
                    case L' ':
                        patch.diffs.push_back(diff_t(operation_t::equal, line));
                        continue;
                    case L'@':
                        break;
                    default:
                        throw string_t(traits::cs(L"Invalid patch mode '") + (sign + (traits::cs(L"' in: ") + line)));
                    }
                    break;
                }

                patches.push_back(patch);
            }
        }
        return patches;
    }

private:
    static inline string_t safe_mid(const string_t &str, size_t pos) {
        return (pos == str.length()) ? string_t() : str.substr(pos);
    }

private:
    static inline string_t safe_mid(const string_t &str, size_t pos, size_t len) {
        return (pos == str.length()) ? string_t() : str.substr(pos, len);
    }

private:
    static string_t as_string(ssize_t n) {
        string_t str;
        bool negative = false;
        size_t l = 0;
        if (n < 0) {n = -n; ++l; negative = true;}
        ssize_t n_ = n; do { ++l; } while ((n_ /= 10) > 0);
        str.resize(l);
        typename string_t::iterator s = str.end();
        const wchar_t digits[] = L"0123456789";
        do { *--s = traits::from_wchar(digits[n % 10]); } while ((n /= 10) > 0);
        if (negative) *--s = traits::from_wchar(L'-');
        return str;
    }

    static ssize_t as_int(const string_t& str) { return traits::as_int(str.c_str()); }

    static bool is_control(char_t c) { switch (traits::to_wchar(c)) { case L'\n': case L'\r': return true; } return false; }

    static typename string_t::size_type next_token(const string_t& str, char_t delim, typename string_t::const_pointer off) {
        typename string_t::const_pointer p = off, end = str.c_str() + str.length();
        for (; p != end; ++p) if (*p == delim) break;
        return p - off;
    }

    static void append_percent_encoded(string_t& s1, const string_t& s2) {
        const wchar_t safe_chars[] = L"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_.~ !*'();/?:@&=+$,#";

        size_t safe[0x100], i;
        for (i = 0; i < 0x100; ++i) safe[i] = 0;
        for (i = 0; i < sizeof(safe_chars) / sizeof(wchar_t); ++i) safe[safe_chars[i]] = i + 1;

        ssize_t n = 0;
        typename traits::utf32_t u;
        typename string_t::const_pointer c = s2.c_str(), end = c + s2.length();
        while (c != end) {
            c = traits::to_utf32(c, end, u);
            n += u >= 0x10000? 12 : u >= 0x800? 9 : u >= 0x80? 6 : safe[static_cast<unsigned char>(u)]? 1 : 3;
        }
        if (n == ssize_t(s2.length()))
            s1.append(s2);
        else {
            s1.reserve(s1.size() + n);
            unsigned char utf8[4];
            for (c = s2.c_str(); c != end;) {
                c = traits::to_utf32(c, end, u);
                unsigned char* pt = utf8;
                if (u < 0x80)
                    *pt++ = (unsigned char)u;
                else if (u < 0x800) {
                    *pt++ = (unsigned char)((u >> 6) | 0xC0);
                    *pt++ = (unsigned char)((u & 0x3F) | 0x80);
                }
                else if (u < 0x10000) {
                    *pt++ = (unsigned char)((u >> 12) | 0xE0);
                    *pt++ = (unsigned char)(((u >> 6) & 0x3F) | 0x80);
                    *pt++ = (unsigned char)((u & 0x3F) | 0x80);
                }
                else {
                    *pt++ = (unsigned char)((u >> 18) | 0xF0);
                    *pt++ = (unsigned char)(((u >> 12) & 0x3F) | 0x80);
                    *pt++ = (unsigned char)(((u >> 6) & 0x3F) | 0x80);
                    *pt++ = (unsigned char)((u & 0x3F) | 0x80);
                }

                for (const unsigned char* p = utf8; p < pt; ++p)
                    if (safe[*p])
                        s1 += traits::from_wchar(safe_chars[safe[*p] - 1]);
                    else {
                        s1 += traits::from_wchar(L'%');
                        s1 += traits::from_wchar(safe_chars[(*p & 0xF0) >> 4]);
                        s1 += traits::from_wchar(safe_chars[*p & 0xF]);
                    }
            }
        }
    }

    static unsigned hex_digit_value(char_t c) {
        switch (traits::to_wchar(c))
        {
        case L'0': return 0;
        case L'1': return 1;
        case L'2': return 2;
        case L'3': return 3;
        case L'4': return 4;
        case L'5': return 5;
        case L'6': return 6;
        case L'7': return 7;
        case L'8': return 8;
        case L'9': return 9;
        case L'A': case L'a': return 0xA;
        case L'B': case L'b': return 0xB;
        case L'C': case L'c': return 0xC;
        case L'D': case L'd': return 0xD;
        case L'E': case L'e': return 0xE;
        case L'F': case L'f': return 0xF;
        }
        throw string_t(string_t(traits::cs(L"Invalid character: ")) + c);
    }

    static void percent_decode(string_t& str) {
        typename string_t::iterator s2 = str.begin(), s3 = s2, s4 = s2;
        for (typename string_t::const_pointer s1 = str.c_str(), end = s1 + str.size(); s1 != end; ++s1, ++s2)
            if (traits::to_wchar(*s1) != L'%')
                *s2 = *s1;
            else {
                char_t d1 = *++s1;
                *s2 = char_t((hex_digit_value(d1) << 4) + hex_digit_value(*++s1));
            }
        while (s3 != s2) {
            unsigned u = *s3;
            if (u < 0x80)
                ;
            else if ((u >> 5) == 6) {
                if (++s3 == s2 || (*s3 & 0xC0) != 0x80) continue;
                u = ((u & 0x1F) << 6) + (*s3 & 0x3F);
            }
            else if ((u >> 4) == 0xE) {
                if (++s3 == s2 || (*s3 & 0xC0) != 0x80) continue;
                u = ((u & 0xF) << 12) + ((*s3 & 0x3F) << 6);
                if (++s3 == s2 || (*s3 & 0xC0) != 0x80) continue;
                u += *s3 & 0x3F;
            }
            else if ((u >> 3) == 0x1E) {
                if (++s3 == s2 || (*s3 & 0xC0) != 0x80) continue;
                u = ((u & 7) << 18) + ((*s3 & 0x3F) << 12);
                if (++s3 == s2 || (*s3 & 0xC0) != 0x80) continue;
                u += (*s3 & 0x3F) << 6;
                if (++s3 == s2 || (*s3 & 0xC0) != 0x80) continue;
                u += *s3 & 0x3F;
            }
            else {
                ++s3;
                continue;
            }
            s4 = traits::from_utf32(u, s4);
            ++s3;
        }
        if (s4 != str.end()) str.resize(s4 - str.begin());
    }

    static string_t right(const string_t& str, typename string_t::size_type n) { return str.substr(str.size() - n); }
};


template <class char_t, class utf32_type = unsigned>
struct diff_match_patch_utf32_direct {

    typedef utf32_type utf32_t;

    template <class iterator> static iterator to_utf32(iterator i, iterator, utf32_t& u)  {
        u = *i++;
        return i;
    }

    template <class iterator> static iterator from_utf32(utf32_t u, iterator o) {
        *o++ = static_cast<char_t>(u);
        return o;
    }
};


template <class char_t, class utf32_type = unsigned>
struct diff_match_patch_utf32_from_utf16 {

    typedef utf32_type utf32_t;

    static const unsigned utf16_surrogate_min = 0xD800u;
    static const unsigned utf16_surrogate_max = 0xDFFFu;
    static const unsigned utf16_high_surrogate_max = 0xDBFFu;
    static const unsigned utf16_low_surrogate_min = 0xDC00u;
    static const unsigned utf16_surrogate_offset = (utf16_surrogate_min << 10) + utf16_high_surrogate_max - 0xFFFFu;

    template <class iterator> static iterator to_utf32(iterator i, iterator end, utf32_t& u) {
        u = *i++;
        if (utf16_surrogate_min <= u && u <= utf16_high_surrogate_max && i != end)
            u = (u << 10) + *i++ - utf16_surrogate_offset;
        return i;
    }

    template <class iterator> static iterator from_utf32(utf32_t u, iterator o)
    {
        if (u > 0xFFFF) {
            *o++ = static_cast<char_t>((u >> 10) + utf16_surrogate_min - (0x10000 >> 10));
            *o++ = static_cast<char_t>((u & 0x3FF) + utf16_low_surrogate_min);
        }
        else
            *o++ = static_cast<char_t>(u);
        return o;
    }
};


template <>
struct diff_match_patch_traits<wchar_t> : diff_match_patch_utf32_from_utf16<wchar_t> {
    static bool is_alnum(wchar_t c) { return std::iswalnum(c)? true : false; }
    static bool is_digit(wchar_t c) { return std::iswdigit(c)? true : false; }
    static bool is_space(wchar_t c) { return std::iswspace(c)? true : false; }
    static ssize_t as_int(const wchar_t* s) { return static_cast<ssize_t>(std::wcstol(s, NULL, 10)); }
    static wchar_t from_wchar(wchar_t c) { return c; }
    static wchar_t to_wchar(wchar_t c) { return c; }
    static const wchar_t* cs(const wchar_t* s) { return s; }
    static const wchar_t eol = L'\n';
    static const wchar_t tab = L'\t';
};


template <>
struct diff_match_patch_traits<char> : diff_match_patch_utf32_direct<char>
{
    static bool is_alnum(char c) { return std::isalnum((unsigned char)c)? true : false; }
    static bool is_digit(char c) { return std::isdigit((unsigned char)c)? true : false; }
    static bool is_space(char c) { return std::isspace((unsigned char)c)? true : false; }
    static ssize_t as_int(const char* s) { return std::atoi(s); }
    static char from_wchar(wchar_t c) { return static_cast<char>(c); }
    static wchar_t to_wchar(char c) { return static_cast<wchar_t>(c); }
    static std::string cs(const wchar_t* s) { return std::string(s, s + wcslen(s)); }
    static const char eol = '\n';
    static const char tab = '\t';
};

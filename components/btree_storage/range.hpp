#pragma once
#include <list>

namespace components::btree {

    class data_ranges_t {
    public:
        using range_t = std::pair<std::size_t, std::size_t>;
        using ranges_t = std::list<range_t>;

        data_ranges_t() = default;
        void add_range(const range_t& range);
        void clear();
        [[nodiscard]] bool empty() const;
        void sort();
        void reverse_sort();
        [[nodiscard]] const ranges_t& ranges() const;

    private:
        ranges_t ranges_;

        static bool is_cross_(const range_t& r1, const range_t& r2);
        static range_t cross_(const range_t& r1, const range_t& r2);
    };

} //namespace components::document
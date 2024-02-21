#pragma once
#include <document/document_view.hpp>
#include <functional>
#include <memory>

using ::components::document::document_ptr;
using ::components::document::document_view_t;

namespace services::storage::sort {

    using components::document::compare_t;

    enum class order { descending = -1, ascending = 1 };

    class sorter_t {
        using function_t = std::function<compare_t(const document_view_t*, const document_view_t*)>;

    public:
        explicit sorter_t() = default;
        explicit sorter_t(const std::string& key, order order_ = order::ascending);

        void add(const std::string& key, order order_ = order::ascending);
        bool operator()(const document_view_t* doc1, const document_view_t* doc2) const {
            for (const auto& f : functions_) {
                auto res = f(doc1, doc2);
                if (res < compare_t::equals) {
                    return true;
                } else if (res > compare_t::equals) {
                    return false;
                }
            }
            return true;
        }
        bool operator()(const document_ptr& doc1, const document_ptr& doc2) const {
            const document_view_t view1(doc1);
            const document_view_t view2(doc2);
            return operator()(&view1, &view2);
        }

    private:
        std::vector<function_t> functions_;
    };

} // namespace services::storage::sort

#pragma once
#include <memory>
#include <document/document_view.hpp>
#include <functional>

using ::components::document::document_view_t;

namespace services::storage::sort {

enum class order {
    descending = -1,
    ascending = 1
};

class sorter_t {
    using function_t = std::function<int(const document_view_t*, const document_view_t*)>;

public:
    explicit sorter_t() = default;
    explicit sorter_t(const std::string &key, order order_ = order::ascending);

    void add(const std::string &key, order order_ = order::ascending);
    bool operator ()(const document_view_t *doc1, const document_view_t *doc2) const {
        for (auto f : functions_) {
            auto res = f(doc1, doc2);
            if (res < 0) return true;
            else if (res > 0) return false;
        }
        return true;
    }

private:
    std::vector<function_t> functions_;
};

}

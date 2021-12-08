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
    using function_t = std::function<int(const document_view_t&, const document_view_t&)>;

public:
    explicit sorter_t(const std::string &key, order order_ = order::ascending);

    void add(const std::string &key, order order_ = order::ascending);
    bool less(const document_view_t &doc1, const document_view_t &doc2) const;

private:
    std::vector<function_t> functions_;
};

}

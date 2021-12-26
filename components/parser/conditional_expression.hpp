#pragma once
#include "document/document.hpp"
#include "document/document_view.hpp"

namespace components::parser {

using components::document::document_t;
using components::document::document_view_t;
using ::document::impl::value_t;


class conditional_expression {
public:
    conditional_expression() = default;
    explicit conditional_expression(const std::string &key);
    virtual ~conditional_expression() = default;

    template <class T> bool is_fit(const T &doc) {
        return check(prepare_document_(doc));
    }

    virtual bool is_union() const;
    virtual void set_value(const value_t *);

protected:
    std::string key_;

private:
    std::string full_key_;

    virtual bool check(const document_view_t &) const = 0;
    virtual bool check(const document_t &) const = 0;

    const document_view_t prepare_document_(const document_view_t &doc);
    const document_t prepare_document_(const document_t &doc);
};
using conditional_expression_ptr = std::shared_ptr<conditional_expression>;


class find_condition_t : public conditional_expression {
public:
    find_condition_t() = default;
    find_condition_t(std::vector<conditional_expression_ptr> &&conditions);
    find_condition_t(const std::vector<conditional_expression_ptr> &conditions);
    void add(conditional_expression_ptr &&condition);
    bool check(const document_view_t &doc) const override;
    bool check(const document_t &doc) const override;
    bool is_union() const override;
protected:
    std::vector<conditional_expression_ptr> conditions_;
private:
    template <class T> bool check_(const T &doc) const;
};
using find_condition_ptr = std::shared_ptr<find_condition_t>;


template <class T, class ...Targs>
std::shared_ptr<T> make_condition(Targs ...args) {
    return std::make_shared<T>(args...);
}

}

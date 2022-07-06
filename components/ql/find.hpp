#include <vector>
#include <string>
#include <utility>

enum class condition_type : std::uint8_t {
    novalid,
    eq,
    ne,
    gt,
    lt,
    gte,
    lte,
    regex,
    any,
    all,
    union_and,
    union_or,
    union_not
};


using ::document::impl::value_type;
using ::document::impl::value_t;
using ::document::impl::array_t;

/// SIMPLES VALUES ///

template <class T>
class find_condition_simple {
    find_condition_simple() = delete;
    T value_;
    std::vector<conditional_expression_ptr>
};



template <class T>
class find_condition_eq  {
    T value_;
    std::vector<conditional_expression_ptr> conditions_;
};


template <class T>
class find_condition_ne  {
    T value_;
    std::vector<conditional_expression_ptr> conditions_;
};


template <class T>
class find_condition_gt{
    T value_;
    std::vector<conditional_expression_ptr> conditions_;
};


template <class T>
class find_condition_lt  {
    T value_;
    std::vector<conditional_expression_ptr> conditions_;
};


template <class T>
class find_condition_gte  {
    T value_;
    std::vector<conditional_expression_ptr> conditions_;
};


template <class T>
class find_condition_lte  {
    T value_;
    std::vector<conditional_expression_ptr> conditions_;
};


template <class T>
class find_condition_between {
public:
    find_condition_between(const std::string &key, const T &value1, const T &value2)
        : value_ (value1)
        , value2_(value2) {}

private:
    T value_;
    T value2_;
    std::vector<conditional_expression_ptr> conditions_;
};


class find_condition_regex  {
    std::string value ;
    std::vector<conditional_expression_ptr> conditions_;
};


/// ARRAY VALUES ///

template <class T>
class find_condition_array  {
    std::string key_;
    std::vector<T> values_;
};


template <class T>
class find_condition_any  {
    std::string key_;
    std::vector<T> values_;


};


template <class T>
class find_condition_all  {
    std::string key_;
    std::vector<T> values_;
};


/// COMPLEX VALUES ///

class find_condition_and  {
public:
    std::vector<conditional_expression_ptr>
};


class find_condition_or  {
    std::vector<conditional_expression_ptr>
};


class find_condition_not  {
    std::vector<conditional_expression_ptr>
};

struct find_statement {

    std::vector<find_statement*> exps_;
};


conditional_expression_ptr make_find_condition(condition_type type, const std::string &key, const value_t *value);

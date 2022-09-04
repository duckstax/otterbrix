#pragma once
/// maybe limit_t -> predicate
class limit_t {
public:
    limit_t(){}
    limit_t(int data ):limit_(data){}
private:
    int limit_ = 0;
};

#pragma once
#include <system_error>
class result_insert_one {
public:
    result_insert_one()=default;
    result_insert_one(bool result ):status(result){}
    bool status = false;
    //TODO: except or error_code
};

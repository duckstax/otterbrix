#include "parsenodes.h"
#include <components/sql/parser/pg_functions.h>

Value* makeInteger(long i) {
    Value* v = makeNode(Value);

    v->type = T_Integer;
    v->val.ival = i;
    return v;
}

extern Value* makeFloat(char* numericStr) {
    Value* v = makeNode(Value);

    v->type = T_Float;
    v->val.str = numericStr;
    return v;
}

extern Value* makeString(char* str) {
    Value* v = makeNode(Value);

    v->type = T_String;
    v->val.str = str;
    return v;
}

extern Value* makeBitString(char* str) {
    Value* v = makeNode(Value);

    v->type = T_BitString;
    v->val.str = str;
    return v;
}

#include "json5.hpp"
#include <sstream>

using namespace std;

namespace storage {

static inline bool is_new_line(int c) {
    return (c == '\n' || c == '\r');
}


class json5converter {
public:
    json5converter(istream &in, ostream &out);

    void parse();

private:
    void parse_value();
    void parse_constant(const char *ident);
    void parse_number();
    void parse_string();
    void parse_sequence(bool is_object);

    char peek_token();
    char peek();
    char get();
    void skip_comment();
    void fail(const char *error);

    istream &_in;
    ostream &_out;
    string::size_type _pos {0};
};

json5converter::json5converter(istream &in, ostream &out)
    : _in(in)
    , _out(out)
{}

void json5converter::parse() {
    parse_value();
    if (peek_token() != 0)
        fail("Unexpected characters after end of value");
}

void json5converter::parse_value() {
    switch(peek_token()) {
    case 'n':
        parse_constant("null");
        break;
    case 't':
        parse_constant("true");
        break;
    case 'f':
        parse_constant("false");
        break;
    case '-':
    case '+':
    case '.':
    case '0': case '1': case '2': case '3': case '4':
    case '5': case '6': case '7': case '8': case '9':
        parse_number();
        break;
    case '"':
    case '\'':
        parse_string();
        break;
    case '[':
        parse_sequence(false);
        break;
    case '{':
        parse_sequence(true);
        break;
    default:
        fail("Invalid start of JSON5 value");
    }
}

void json5converter::parse_constant(const char *ident) {
    auto cp = ident;
    while (*cp && get() == *cp)
        ++cp;
    char c = peek();
    if (*cp || isalnum(c) || c == '$' || c == '_')
        fail("Unknown identifier");
    _out << ident;
}

void json5converter::parse_number() {
    char c = get();
    if (c == '.')
        _out << "0.";
    else if (c != '+')
        _out << c;
    while (true) {
        c = peek();
        if (isdigit(c) || c == '.' || c == 'e' || c == 'E' || c == '-' || c == '+')
            _out << get();
        else
            break;
    }
}

void json5converter::parse_string() {
    _out << '"';
    const char quote = get();
    char c;
    while (quote != (c = get())) {
        if (c == '"') {
            _out << "\\\"";
        } else if (c == '\\') {
            char esc = get();
            if (!is_new_line(esc)) {
                if (esc != '\'')
                    _out << '\\';
                _out << esc;
            }
        } else {
            _out << c;
        }
    }
    _out << '"';
}

void json5converter::parse_sequence(bool is_object) {
    _out << get();
    const char close_bracket = (is_object ? '}' : ']');
    bool first = true;
    char c;
    while (close_bracket != (c = peek_token())) {
        if (first)
            first = false;
        else
            _out << ",";
        if (is_object) {
            if (c == '"' || c == '\'') {
                parse_string();
            } else if (isalpha(c) || c == '_' || c == '$') {
                _out << '"' << get();
                while (true) {
                    c = peek();
                    if (isalnum(c) || c == '_')
                        _out << get();
                    else
                        break;
                }
                _out << '"';
            } else {
                fail("Invalid key");
            }
            if (peek_token() != ':')
                fail("Expected ':' after key");
            _out << get();
        }
        parse_value();
        if (peek_token() == ',')
            get();
        else if (peek_token() != close_bracket)
            fail("Unexpected token after array/object item");
    }
    _out << get();
}

char json5converter::peek_token() {
    while (true) {
        char c = peek();
        if (c == 0) {
            return c;
        } else if (isspace(c)) {
            get();
        } else if (c == '/') {
            skip_comment();
        } else {
            return c;
        }
    }
}

void json5converter::skip_comment() {
    char c;
    get();
    switch (get()) {
    case '/':
        do {
            c = peek();
            if (c)
                get();
        } while (c != 0 && !is_new_line(c));
        break;
    case '*': {
        bool star;
        c = 0;
        do {
            star = (c == '*');
            c = get();
        } while (!(star && c=='/'));
        break;
    }
    default:
        fail("Syntax error after '/'");
    }
}

char json5converter::peek() {
    int c = _in.peek();
    return (c < 0) ? 0 : char(c);
}

char json5converter::get() {
    int c = _in.get();
    if (_in.eof())
        fail("Unexpected end of JSON5");
    ++_pos;
    return char(c);
}

void json5converter::fail(const char *error) {
    stringstream message;
    message << error << " (at :" << _pos << ")";
    throw json5_error(message.str(), _pos);
}



json5_error::json5_error(const std::string &what, std::string::size_type input_pos)
    : std::runtime_error(what)
    , input_pos(input_pos)
{}

void convert_json5(istream &in, ostream &out) {
    json5converter(in, out).parse();
}

std::string convert_json5(const std::string &json5) {
    stringstream in(json5), out;
    convert_json5(in, out);
    return out.str();
}

}

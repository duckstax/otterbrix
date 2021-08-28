#include <cstdint>
#include <iostream>
#include <memory>
#include <any>

int main() {
    std::cerr << sizeof(std::uint64_t) << std::endl;
    std::cerr << sizeof(std::int64_t) << std::endl;
    std::cerr << sizeof(double) << std::endl;
    std::cerr << sizeof(float) << std::endl;
    std::cerr << sizeof(void*) << std::endl;
    std::cerr << sizeof(std::unique_ptr<std::string>) << std::endl;
    std::cerr << sizeof(std::any) << std::endl;
    std::cerr << sizeof(std::pair<int,int>) << std::endl;
    std::cerr << sizeof(std::pair<std::unique_ptr<std::string>,std::unique_ptr<std::string>>) << std::endl;

}
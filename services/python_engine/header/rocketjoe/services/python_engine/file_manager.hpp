#pragma  once

#include <unordered_map>
#include <fstream>
#include <iostream>

class file_view final {
private:
    using storage_t = std::map<std::size_t, std::string>;
public:
    file_view(const std::string& path):file_(path){}

    auto read()-> storage_t {
        if (file_) {
            for (std::size_t line_number = 0; !file_.eof(); line_number++) {
                std::string line;

                std::getline(file_, line);
                file_content[line_number] = line;
            }
        }
        return file_content;
    }

private:
    std::ifstream file_;
    storage_t file_content;
};

class file_manager final  {
public:
    auto open(const std::string& path) -> file_view& {
        file_view tmp(path);
        auto result = files.emplace(path,std::move(tmp));
        return result.first->second;
    }
private:
    std::unordered_map<std::string,file_view> files;
};
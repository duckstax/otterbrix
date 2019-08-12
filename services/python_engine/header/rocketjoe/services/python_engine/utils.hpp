#pragma once

namespace rocketjoe { namespace services { namespace python_engine {

inline auto add_file_read(py::module &pyrocketjoe){

    pyrocketjoe.def(
            "file_read",
            [](const std::string &path) -> std::map<std::size_t, std::string> {
                std::ifstream file(path);
                std::map<std::size_t, std::string> file_content;

                if (!file)
                    return file_content;

                for (std::size_t line_number = 0; !file.eof(); line_number++) {
                    std::string line;

                    std::getline(file, line);
                    file_content[line_number] = line;
                }

                return file_content;
            }
    );
}

}}}
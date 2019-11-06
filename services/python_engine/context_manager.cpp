#include <rocketjoe/services/python_engine/context_manager.hpp>
#include <rocketjoe/services/python_engine/data_set_manager.hpp>

namespace rocketjoe { namespace services { namespace python_engine {

        auto context::next() -> data_set * {
            archive_data_set_.emplace(std::make_unique<data_set>());
            return archive_data_set_.top().get();
        }

        auto context::top() -> rocketjoe::services::python_engine::data_set * {
            return archive_data_set_.top().get();
        }

        auto context::open_file(const boost::filesystem::path &path) {
                return file_manager_.open(path);
        }

}}}
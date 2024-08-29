#include "otterbrix.hpp"

namespace otterbrix {

    class connector_t {
    public:
        explicit connector_t(boost::intrusive_ptr<otterbrix_t> instance);
        connector_t(const connector_t&) = delete;
        connector_t& operator=(const connector_t&) = delete;

        void execute_async(const std::string& query);
        components::cursor::cursor_t_ptr execute(const std::string& query);
        components::cursor::cursor_t_ptr cursor();
        void close();

    private:
        boost::intrusive_ptr<otterbrix_t> instance_;
        components::cursor::cursor_t_ptr cursor_store_{nullptr};
    };

} // namespace otterbrix
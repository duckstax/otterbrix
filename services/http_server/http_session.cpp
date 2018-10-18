#include <rocketjoe/services/http_server/http_session.hpp>
#include <boost/beast/core/string.hpp>

namespace rocketjoe { namespace services { namespace http_server {

            constexpr const  char* dispatcher = "dispatcher";
            constexpr const  char* router = "lua_engine";


            template<
                    class Body,
                    class Allocator
            >
            void handle_request(
                    http::request<Body, http::basic_fields<Allocator>>&& req,
                    api::transport_id id,
                    http_context& context
            ) {

                auto http_transport_ = std::make_shared<api::http>(id);

                http_transport_->method(std::string(req.method_string()));

                http_transport_->uri(std::string(req.target()));

                for(auto const& field : req) {

                    std::string header_name(field.name_string());
                    std::string header_value(field.value());
                    http_transport_->header(std::move(header_name),std::move(header_value));
                }

                http_transport_->body(req.body());

                api::transport http_data(std::move(http_transport_)) ;


                if(req.target() == "/system"){
                    context.send(goblin_engineer::message("object_storage",dispatcher,{std::move(http_data)}));
                    return;
                }



                if(context.check_url(std::string(req.target()))){
                    context.send(goblin_engineer::message("object_storage",dispatcher,{std::move(http_data)}));
                    return;
                }

                context.send(goblin_engineer::message(router,dispatcher,{std::move(http_data)}));

            }

            http_session::http_session(tcp::socket socket,api::transport_id id, http_context& context) :
                    socket_(std::move(socket)),
                    strand_(socket_.get_executor()),
                    timer_(socket_.get_executor().context(),(std::chrono::steady_clock::time_point::max) ()),
                    queue_(*this),
                    context(context),
                    id(id){
            }

            void http_session::run() {
                // Make sure we run on the strand
                if (!strand_.running_in_this_thread())
                    return boost::asio::post(
                            boost::asio::bind_executor(
                                    strand_,
                                    std::bind(
                                            &http_session::run,
                                            shared_from_this())));

                // Run the timer. The timer is operated
                // continuously, this simplifies the code.
                on_timer({});

                do_read();
            }

            void http_session::on_timer(boost::system::error_code ec) {
                if (ec && ec != boost::asio::error::operation_aborted) {
                    return fail(ec, "timer");
                }

                // Verify that the timer really expired since the deadline may have moved.
                if (timer_.expiry() <= std::chrono::steady_clock::now()) {
                    // Closing the socket cancels all outstanding operations. They
                    // will complete with boost::asio::error::operation_aborted
                    socket_.shutdown(tcp::socket::shutdown_both, ec);
                    socket_.close(ec);
                    return;
                }

                // Wait on the timer
                timer_.async_wait(
                        boost::asio::bind_executor(
                                strand_,
                                std::bind(
                                        &http_session::on_timer,
                                        shared_from_this(),
                                        std::placeholders::_1)));
            }

            void http_session::on_read(boost::system::error_code ec) {
                // Happens when the timer closes the socket
                if (ec == boost::asio::error::operation_aborted)
                    return;

                // This means they closed the connection
                if (ec == http::error::end_of_stream)
                    return do_close();

                if (ec)
                    return fail(ec, "read");

                // See if it is a WebSocket Upgrade
                if (websocket::is_upgrade(req_)) {
                    // Create a WebSocket websocket_session by transferring the socket
                    std::make_shared<websocket_session>(std::move(socket_),context)->do_accept(std::move(req_));
                    return;
                }

                // Send the response
                handle_request(std::move(req_),id,context);

                // If we aren't at the queue limit, try to pipeline another request
                if (!queue_.is_full()) {
                    do_read();
                }
            }

            void http_session::on_write(boost::system::error_code ec, bool close) {
                // Happens when the timer closes the socket
                if (ec == boost::asio::error::operation_aborted)
                    return;

                if (ec)
                    return fail(ec, "write");

                if (close) {
                    // This means we should close the connection, usually because
                    // the response indicated the "Connection: close" semantic.
                    return do_close();
                }

                // Inform the queue that a write completed
                if (queue_.on_write()) {
                    // Read another request
                    do_read();
                }
            }

            void http_session::do_close() {
                // Send a TCP shutdown
                boost::system::error_code ec;
                socket_.shutdown(tcp::socket::shutdown_send, ec);

                // At this point the connection is closed gracefully
            }

            void http_session::do_read() {
                // Set the timer
                timer_.expires_after(std::chrono::seconds(15));

                // Make the request empty before reading,
                // otherwise the operation behavior is undefined.
                req_ = {};

                // Read a request
                http::async_read(socket_, buffer_, req_,
                                 boost::asio::bind_executor(
                                         strand_,
                                         std::bind(
                                                 &http_session::on_read,
                                                 shared_from_this(),
                                                 std::placeholders::_1)));
            }

            void http_session::write(std::unique_ptr<api::transport_base> ptr) {
                auto*http = static_cast<api::http*>(ptr.release());
                http::response<http::string_body > res{http::status::ok, 11};
                res.body()=http->body();
                for(auto&&i:(*http)){
                    res.set(i.first,i.second);
                }
                queue_(std::move(res));
            }

            http_session::~http_session()  = default;

            bool http_session::queue::on_write() {
                BOOST_ASSERT(!items_.empty());
                auto const was_full = is_full();
                items_.erase(items_.begin());
                if (!items_.empty())
                    (*items_.front())();
                return was_full;
            }

            bool http_session::queue::is_full() const {
                return items_.size() >= limit;
            }

            http_session::queue::queue(http_session &self) : self_(self) {
                static_assert(limit > 0, "queue_vm limit must be positive");
                items_.reserve(limit);
            }
        }}}
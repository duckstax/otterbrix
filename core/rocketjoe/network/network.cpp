#include <rocketjoe/network/network.hpp>
#include <iostream>

namespace rocketjoe { namespace network {

        void fail(boost::system::error_code ec, char const *what) {
            std::cerr << what << ": " << ec.message() << "\n";
        }

}}
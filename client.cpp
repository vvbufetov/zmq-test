#include <thread>
#include <string>

#include "zmq.hpp"
#include "zhelpers.hpp"
#include "defs.h"

//  This is our client task class.
//  It connects to the server, and then sends a request for subscriptions.
//  It collects responses as they arrive, and it prints them out.

class client_task {
public:
    client_task(const std::string connect_string, const std::string filter)
        : ctx_(1),
          filter_(filter),
          connect_string_(connect_string),
          socket_(ctx_, ZMQ_DEALER)
    {}

    void start() {
        socket_.connect(connect_string_);
        zmq::pollitem_t items[] = {{socket_, 0, ZMQ_POLLIN, 0}};
        int retries = 5;
        try {
            // initialize the subscription
            if (!s_send(socket_, filter_)) {
                std::runtime_error("zmq_send subscription failed");
            }
            while (!s_interrupted) {
                // 1000 milliseconds
                zmq::poll(items, 1, 1000);
                if (items[0].revents & ZMQ_POLLIN) {
                    std::string line = s_recv(socket_);
                    if (line.compare((const char *)HEARTBEAT_MESSAGE) == 0) {
                        std::cerr << "HB\n";
                        s_send(socket_, filter_);
                    } else {
                        std::cout << s_recv(socket_) << "\n";
                    }
                    retries = 5;
                }
                else if (--retries == 0) {
                    std::cerr << "timed out\n";
                    break;
                }
                // timeout
                else {
                    std::cerr << ".";
                }
            }
        }
        catch (std::exception &e) {
            std::cerr << e.what() << "\n";
        }
        socket_.setsockopt(ZMQ_LINGER, 1);
    }

private:
    zmq::context_t ctx_;
    std::string filter_;
    std::string connect_string_;
    zmq::socket_t socket_;
};

int main (int argc, char ** argv)
{
    if (argc != 3) {
        std::cerr << "Usage: client \"tcp://host:port\" \"filter_text\"\n";
        return (-1);
    }

    s_catch_signals();

    client_task ct(argv[1], argv[2]);
    std::thread t(std::bind(&client_task::start, &ct));

    t.join();
    return 0;
}

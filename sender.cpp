#include <thread>
#include <string>

#include "zmq.hpp"
#include "zhelpers.hpp"

class sender_task {
public:
    sender_task(const std::string connect_string)
        : ctx_(1),
          connect_string_(connect_string),
          frontend_(ctx_, ZMQ_PUSH)
    {}

    void start() {
        frontend_.bind(connect_string_);
        uint64_t cnt = 0;
        try {
            while (!std::cin.eof() && !s_interrupted) {
                std::string line;
                std::getline(std::cin, line);
                if (std::cin.eof()) break;
                if (s_send(frontend_, line)) cnt ++;
            }
            if (s_interrupted) {
                frontend_.setsockopt(ZMQ_LINGER,1);
            }
        }
        catch (std::exception &e) {
            std::cerr << e.what() << "\n";
        }
        std::cout << "sent: " << cnt << " lines\n";
    }

private:
    zmq::context_t ctx_;
    std::string connect_string_;
    zmq::socket_t frontend_;
};


int main (int argc, char ** argv)
{
    if (argc != 2) {
        std::cerr << "Usage: sender \"tcp://*:port\"\n";
        return (-1);
    }

//    s_catch_signals();

    sender_task st(argv[1]);
    std::thread t(std::bind(&sender_task::start, &st));

    t.join();
    return 0;
}

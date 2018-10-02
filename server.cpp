//  Asynchronous client-to-server (DEALER to ROUTER)
//
//  While this example runs in a single process, that is to make
//  it easier to start and stop the example. Each task has its own
//  context and conceptually acts as a separate process.

#include <unordered_map>
#include <thread>
//#include <memory>
//#include <functional>
#include <string>
#include <stdexcept>

#include "zmq.hpp"
#include "zhelpers.hpp"




// This is simple worker server. It reads lines from ventilator socket,
// checks subscription list and sends message copies to clients.
// Also it receives and stores the subscription filter from each clients.

class server_task {
    struct ClientId {
        char id[256];
        size_t size = 0;
        ClientId() {};
        ClientId(const zmq::message_t & msg) {
            size = msg.size();
            if (!size) throw std::runtime_error("invalid id size");
            if (size > 256) size = 256;
            memcpy(id, msg.data(), size);
        }
        void send(zmq::socket_t & socket, int flags_ = 0) {
            zmq::message_t msg(id, size);
            socket.send(msg, flags_);
        }
    };

    struct Subscription {
        bool active = false;
        ClientId id;
        std::string filter;
        Subscription () {};
        Subscription(const ClientId & id_, const zmq::message_t & msg)
            : active(true),
              id(id_),
              filter(std::string(static_cast<const char*>(msg.data()), msg.size())) {}
        bool match(const std::string & str) {
            return active && (str.find( filter ) != std::string::npos);
        }
    };

public:
    server_task(const std::string sender_port, const std::string clients_port)
        : ctx_(1),
          sender_port_(sender_port),
          clients_port_(clients_port),
          frontend_(ctx_, ZMQ_ROUTER),
          backend_(ctx_, ZMQ_PULL)
    {}

    void start() {
        std::cout << "[S] sender port: " << sender_port_
            << "\n[S] clients port: " << clients_port_ << "\n";

        backend_.bind(sender_port_);
        frontend_.bind(clients_port_);

        std::cout << "[S] started\n";
        try {
            zmq::pollitem_t items[] = {
                {frontend_, 0, ZMQ_POLLIN, 0},
                {backend_, 0, ZMQ_POLLIN, 0}
                };

            while (!s_interrupted) {
                // 100 milliseconds
                zmq::poll(items, 2, 100);
                // frontend (from clients)
                if (items[0].revents & ZMQ_POLLIN) {
                    // TODO: receive and prepare the subscription
                    zmq::message_t id;
                    frontend_.recv(&id);
                    if (!id.more()) {
                        std::cerr << "[S] invalid subscription: " << id << "\n";
                        continue;
                    }
                    zmq::message_t filter_msg;
                    frontend_.recv(&filter_msg);
                    std::cout << "[S] subscripion: " << filter_msg << " from " << id << "\n";
                    subs_[std::hash<std::string>()(id.str())] = Subscription(ClientId(id), filter_msg);
                }
                // backend (from ventilator)
                else if (items[1].revents & ZMQ_POLLIN) {
                    // TODO: filter and resend
                    std::string line = s_recv(backend_);
                    std::cout << "[S] line: " << line << "\n";
                    for (auto item : subs_) {
                        if (item.second.match(line)) {
                            item.second.id.send(frontend_, ZMQ_SNDMORE);
                            s_send(frontend_, line);
                        }
                    }
                }
                // timeout
                {
                    // TODO: check subscriptions
                }
            }
        }
        catch (std::exception &e) {}
    }



private:
    zmq::context_t ctx_;
    std::string sender_port_;
    std::string clients_port_;
    zmq::socket_t backend_;
    zmq::socket_t frontend_;
    std::unordered_map<size_t, Subscription> subs_;
};


int main (int argc, char ** argv)
{
    if (argc != 3) {
        std::cerr << "Usage: server \"tcp://host:port\" \"tcp://host:port\"\n"
            "\tthe first port is used for the sender\n"
            "\tthe second is for clients\n";
        return (-1);
    }

    s_catch_signals();

    server_task st(argv[1], argv[2]);
    std::thread t(std::bind(&server_task::start, &st));

    t.join();
    return 0;
}

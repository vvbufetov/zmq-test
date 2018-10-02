//  Asynchronous client-to-server (DEALER to ROUTER)
//
//  While this example runs in a single process, that is to make
//  it easier to start and stop the example. Each task has its own
//  context and conceptually acts as a separate process.

#include <vector>
#include <unordered_map>
#include <thread>
#include <memory>
#include <functional>
#include <string>
#include <stdexcept>

#include "zmq.hpp"
#include "zhelpers.hpp"


//  This is our client task class.
//  It connects to the server, and then sends a request for subscriptions.
//  It collects responses as they arrive, and it prints them out.

class client_task {
public:
    client_task(zmq::context_t &ctx, const std::string filter)
        : ctx_(1),
          filter_(filter),
          socket_(ctx_, ZMQ_DEALER)
    {}

    void start() {
        std::cout << "[C] filter: " << filter_ << "\n";
        socket_.connect("tcp://localhost:5565");
        std::cout << "[C] connected\n";

        zmq::pollitem_t items[] = {{socket_, 0, ZMQ_POLLIN, 0}};
        try {
            // initialize the subscription
            s_send(socket_, filter_);
            while (!s_interrupted) {
                // 100 milliseconds
                zmq::poll(items, 1, 100);
                if (items[0].revents & ZMQ_POLLIN) {
                    std::cout << "[C](" << filter_ << ") received a message\n";
                    s_dump(socket_);
                }
                // timeout
                else {
                }
            }
        }
        catch (std::exception &e) {}
    }

private:
    zmq::context_t ctx_;
    std::string filter_;
    zmq::socket_t socket_;
};


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
    server_task(zmq::context_t &ctx)
        : ctx_(ctx),
          frontend_(ctx_, ZMQ_ROUTER),
          backend_(ctx_, ZMQ_PULL)
    {}

    void start() {
        std::cout << "[S] context: " << (void*) ctx_ << "\n";

        backend_.connect("inproc://ventilator.ipc");
        frontend_.bind("tcp://*:5565");

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
    zmq::context_t &ctx_;
    zmq::socket_t backend_;
    zmq::socket_t frontend_;
    std::unordered_map<size_t, Subscription> subs_;
};


//  This is our events source.
//  It reads stdin and sends each line as separate packet to worker.

class ventilator_task {
public:
    ventilator_task(zmq::context_t& ctx)
        : ctx_(ctx),
          frontend_(ctx_, ZMQ_PUSH)
    {}

    void start() {
        std::cout << "[V] context: " << (void*) ctx_ << "\n";
        frontend_.bind("inproc://ventilator.ipc");
        std::cout << "[V] started\n";
        try {
            while (!std::cin.eof() && !s_interrupted) {
                std::string line;
                std::getline(std::cin, line);
                std::cout << "[V] send: " << line << "\n";
                s_send(frontend_, line);
            }
            std::cout << "[V] finished\n";
        }
        catch (std::exception &e) {}
    }

private:
    zmq::context_t& ctx_;
    zmq::socket_t frontend_;
};


//  The main thread simply starts several clients and a server, and then
//  waits for the server to finish.
int main (void)
{
    s_catch_signals();

    zmq::context_t ctx;
    client_task ct1(ctx, "A");
    client_task ct2(ctx, "B");
    ventilator_task vt(ctx);
    server_task st(ctx);

    std::thread t1(std::bind(&ventilator_task::start, &vt));
    sleep(1);
    std::thread t2(std::bind(&server_task::start, &st));
    sleep(1);
    std::thread t3(std::bind(&client_task::start, &ct1));
    std::thread t4(std::bind(&client_task::start, &ct2));

    t1.join();
    s_interrupted = 1;
    t2.join();
    t3.join();
    t4.join();

    //getchar();
    return 0;
}

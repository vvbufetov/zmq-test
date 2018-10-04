#include <thread>
#include <string>

#include "zmq.hpp"
#include "zhelpers.hpp"
#include "defs.h"
#include "subscription.h"

#define DELIVERY_TIME 100;

//  This is our client task class.
//  It connects to the server, and then sends a request for subscriptions.
//  It collects responses as they arrive, and it prints them out.

class client_task {
public:
    client_task(const std::string connect_string, const std::string filter)
        : ctx_(1),
          filter_(filter),
          connect_string_(connect_string),
          retry_(5)
    {
        client_id_ = static_cast<TClientID>( rand() );
    }

    zmq::socket_t * connect () {
        zmq::socket_t * sp = new zmq::socket_t(ctx_, ZMQ_DEALER);
        sp->setsockopt(ZMQ_IDENTITY, &client_id_, sizeof(client_id_));
        sp->connect(connect_string_);
        return sp;
    }

    void disconnect (zmq::socket_t * sp) {
        if (sp == nullptr) return;
        std::cerr << "disconnect " << connect_string_ << "\n";
        sp->setsockopt(ZMQ_LINGER, 1);
        delete sp;
    }

    void start() {
        zmq::socket_t * socket_ptr = nullptr;
        try {
            int64_t heartbeat_at = s_clock() + HEARTBEAT_TIMEOUT;
            int64_t subscribe_at = s_clock() + SUBSCRIBE_TIMEOUT - DELIVERY_TIME;
            SubscriptionCommand subscribe_cmd(TSubscriptionCommand::UPDATE, filter_);

            while (!s_interrupted && retry_ > 0) {
                // connecting to the server at the beginning and after loosing a heartbeat
                if (socket_ptr == nullptr) {
                    if (-- retry_ == 0) continue;
                    socket_ptr = connect();
                    heartbeat_at = s_clock() + HEARTBEAT_TIMEOUT;
                    subscribe_at = s_clock() + SUBSCRIBE_TIMEOUT - DELIVERY_TIME;
                    subscribe_cmd.send( *socket_ptr );
                }

                zmq::pollitem_t items[] = {{*socket_ptr, 0, ZMQ_POLLIN, 0}};
                // 10 milliseconds
                int rc = zmq::poll(items, 1, 10);
                if (rc == -1) break;

                int64_t now = s_clock();
                if (items[0].revents & ZMQ_POLLIN) {
                    zmq::message_t msg;
                    socket_ptr->recv( &msg );
                    // the every input packet must be treated as a heartbeat
                    heartbeat_at = now + HEARTBEAT_TIMEOUT;
                    if (msg.size() != 0) {
                        std::string line = std::string(static_cast<char*>(msg.data()), msg.size());
                        std::cout << line << "\n";
                    }
                }

                // checking the server is alive
                if (now > heartbeat_at) {
                    // server is gone
                    disconnect( socket_ptr );
                    socket_ptr = nullptr;
                }

                // update the subscription
                if (socket_ptr != nullptr && now > subscribe_at) {
                    subscribe_cmd.send( *socket_ptr );
                    subscribe_at = s_clock() + SUBSCRIBE_TIMEOUT - DELIVERY_TIME;
                }
            }

            // if the socket is connected and the subscription isn't expired
            if (socket_ptr != nullptr && s_clock() < subscribe_at) {
                SubscriptionCommand unsubscribe_cmd(TSubscriptionCommand::DELETE);
                unsubscribe_cmd.send( *socket_ptr );
            }

        }
        catch (std::exception &e) {
            std::cerr << e.what() << std::endl;
        }

        try{
            disconnect( socket_ptr );
        }
        catch (std::exception &e) {
            std::cerr << e.what() << std::endl;
        }
    }

private:
    zmq::context_t  ctx_;
    std::string     filter_;
    std::string     connect_string_;
    TClientID       client_id_;
    int             retry_;
};

int main (int argc, char ** argv)
{
    if (argc != 3) {
        std::cerr << "Usage: client \"tcp://host:port\" \"filter_text\"\n";
        return (-1);
    }

    s_catch_signals();
    srand( (unsigned) time (NULL) );

    client_task ct(argv[1], argv[2]);
    std::thread t(std::bind(&client_task::start, &ct));

    t.join();
    return 0;
}

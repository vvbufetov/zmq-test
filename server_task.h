#ifndef __SERVER_TASK_H__
#define __SERVER_TASK_H__

#include <string>
#include <unordered_map>

#include "zmq.hpp"
#include "zhelpers.hpp"
#include "subscription.h"

/*

    The list of server_task duties contains:

    * broadcasting subscription commands from clients to all workers
    * tracking the subscription timeout and deleting expired items
    * heartbeating the client connection if there are no new messages to that client from any worker
    * routing data messages from workers and checking for expired subscription keys
    * notifying workers on all expired keys in data messages


    Server ports:

    1. Clients connection (ROUTER)

        Send:       * Data messages

                    * Heartbeat messages

        Receive:    * Subscription commands (UPDATE,DELETE)

    2. Workers connection (ROUTER)

        Receive:    * Data routing multipart message

    3. Broadcast connection (PUB)

        Send:       * Subscription commands (UPDATE,DELETE)


    Packet types:

    "Data message"      - a single-part message that contains a copy of input "Data message"

                                * [<DATA>]

    "Heartbeat"         - a single-part empty message aimed to keep the connection alive

                                * []

    "Subcription message"   - a message with a variable number of parts, first part is always
                              the command, that defines the number and the content of all remaining parts

                                * ["UPDATE"] [<filter>]
                                * ["DELETE"]

    "Data routing message"  - a message from a worker to the server, consists of one data message and
                              one or many address messages with the one client identificator inside

                                * [<DATA>] [<ID>] .... [<ID>]

*/

class server_task {
    struct Timer {
        int64_t     expire_at;
        int64_t     heartbeat_at;
    };
public:
    server_task(const std::string sender_port, const std::string clients_port, const size_t NoW)
        : ctx_(1),
          sender_port_(sender_port),
          clients_port_(clients_port),
          backend_(ctx_, ZMQ_ROUTER),   // connected to workers (recv only)
          frontend_(ctx_, ZMQ_ROUTER),  // connected to clients (send/recv)
          subscription_(ctx_, ZMQ_PUB), // connected to workers (send only)
          number_of_workers_(NoW)
    {}

    void start ();

    void terminate () { terminated_ = true; }

private:
    zmq::context_t      ctx_;
    std::string         sender_port_;
    std::string         clients_port_;
    zmq::socket_t       backend_; // ROUTER
    zmq::socket_t       frontend_; // ROUTER
    zmq::socket_t       subscription_; // PUB
    size_t              number_of_workers_;
    std::unordered_map<TClientID,Timer> timers_;
    bool terminated_ = false;
};

#endif // __SERVER_TASK_H__

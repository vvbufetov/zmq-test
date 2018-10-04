#ifndef __SUBSCRIPTION_H__
#define __SUBSCRIPTION_H__

#include "zmq.hpp"

typedef uint64_t TClientID;

inline
bool unpack_client_id (const zmq::message_t & msg, TClientID& id) {
    size_t size = msg.size();
    if (size != sizeof(TClientID)) return false;
    memcpy(&id, msg.data(), size);
    return true;
}



enum class TSubscriptionCommand : uint8_t {
    UNDEFINED = 0,
    UPDATE = 1,
    DELETE = 2
};

inline
bool unpack_subscription_command(const zmq::message_t & msg, TSubscriptionCommand & cmd) {
    size_t size = msg.size();
    if (size != sizeof(TSubscriptionCommand)) return false;
    memcpy(&cmd, msg.data(), size);
    return true;
}



struct Filter {
    bool        active = false;
    std::string substr;

    Filter () {};

    Filter (const std::string str)
        : active(true),
          substr(str)
    {}

    Filter (const zmq::message_t & msg)
        : active(true),
          substr(std::string(static_cast<const char*>(msg.data()), msg.size()))
    {}

    inline
    bool match(const std::string & str) {
        return active && (str.find( substr ) != std::string::npos);
    }
};



struct SubscriptionCommand {
    TSubscriptionCommand    command = TSubscriptionCommand::UNDEFINED;
    Filter                  filter;

    SubscriptionCommand () {}

    SubscriptionCommand (const TSubscriptionCommand cmd)
        : command(cmd), filter("")
    {}

    SubscriptionCommand (const TSubscriptionCommand cmd, const std::string str)
        : command(cmd), filter(str)
    {}

    inline
    bool empty () { return command == TSubscriptionCommand::UNDEFINED; }

    inline
    void send (zmq::socket_t & socket) {
        zmq::message_t  cmd_msg(&command, sizeof(command));
        if (command == TSubscriptionCommand::UPDATE) {
            socket.send(cmd_msg, ZMQ_SNDMORE);
            s_send(socket, filter.substr);
        }
        else {
            socket.send(cmd_msg);
        }
    }

    inline
    bool recv (zmq::socket_t & socket) {
        zmq::message_t cmd_msg;
        zmq::message_t flt_msg;
        if (!socket.recv(&cmd_msg)) return false;
        if (!unpack_subscription_command(cmd_msg, command)) return false;
        if (command == TSubscriptionCommand::UPDATE && !cmd_msg.more()) return false;
        if (cmd_msg.more()) socket.recv(&flt_msg);
        if (flt_msg.more()) return false;
        if (command == TSubscriptionCommand::UPDATE) filter = Filter(flt_msg);
        return true;
    }
};

#endif // __SUBSCRIPTION_H__

#ifndef __WORKER_TASK_H__
#define __WORKER_TASK_H__

#include "zmq.hpp"
#include "zhelpers.hpp"
#include "subscription.h"
#include <unordered_map>

class worker_task {
public:
    worker_task(zmq::context_t& ctx, const std::string sender_port, const int index)
        : worker_index_(index),
          ctx_(ctx),
          backend_(ctx_, ZMQ_PULL),
          frontend_(ctx_, ZMQ_DEALER),
          subscription_(ctx_, ZMQ_SUB),
          sender_port_(sender_port)
    {}

    void start ();

    void terminate () { terminated_ = true; }

private:
    int            worker_index_;
    zmq::context_t &ctx_;
    zmq::socket_t  backend_;        // to listen to input data
    zmq::socket_t  frontend_;       // to publish data and heartbeats
    zmq::socket_t  subscription_;   // to listen to subscription updates
    std::string    sender_port_;
    std::unordered_map<TClientID, Filter> filters_;
    bool terminated_ = false;
};

#endif // __WORKER_TASK_H__

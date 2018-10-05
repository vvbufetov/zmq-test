#include "defs.h"
#include "server_task.h"
#include "worker_task.h"

#include <thread>
#include <iostream> // debug messages
#include <vector>

#define DEBUG_OUT
#define CHECK_INTERVAL 1000

void server_task::start () {
    std::vector<worker_task*> workers;
    try {
#ifdef DEBUG_OUT
        std::cout << "[S] clients port: " << clients_port_ << "\n";
#endif
        backend_.bind("inproc://workers");
        frontend_.bind(clients_port_);
        subscription_.bind("inproc://subscriptions");

        std::vector<std::thread> worker_threads;
        for (size_t i = 1; i <= number_of_workers_; i++) {
            auto it = workers.emplace(
                workers.end(),
                new worker_task(ctx_, sender_port_, i)
                );
            worker_threads.emplace(worker_threads.end(), std::bind(&worker_task::start, *it));
            s_sleep(100);
        }
#ifdef DEBUG_OUT
        std::cout << "[S] started\n";
#endif
        zmq::pollitem_t items[] = {
            {frontend_, 0, ZMQ_POLLIN, 0},
            {backend_, 0, ZMQ_POLLIN, 0}
        };

        int64_t check_at = s_clock() + CHECK_INTERVAL;

        // some debug stat
        uint64_t routed_packets_cnt = 0;
        uint64_t droped_packets_cnt = 0;

        while (!terminated_) {
            // 10 milliseconds
            zmq::poll(items, 2, 10);
            int64_t now = s_clock();

            // frontend (from clients) /////////////////////////////////////////
            if (items[0].revents & ZMQ_POLLIN) {
                zmq::message_t id_msg;
                frontend_.recv( &id_msg );
                TClientID client;
                SubscriptionCommand cmd;
                bool valid = unpack_client_id(id_msg, client);
                valid &= cmd.recv(frontend_);
                if (valid) {
                    switch (cmd.command) {

                    case TSubscriptionCommand::UPDATE: {
                            auto it = timers_.find(client);
                            if (it == timers_.end()) {
#ifdef DEBUG_OUT
                                std::cerr << "[S] add subscription for " << client << "\n";
#endif
                                timers_[client] = {
                                    now + SUBSCRIBE_TIMEOUT,
                                    now + HEARTBEAT_TIMEOUT - CHECK_INTERVAL
                                    };
                                // confirm the subscription
                                zmq::message_t id_copy;
                                id_copy.copy( &id_msg );
                                frontend_.send( id_msg, ZMQ_SNDMORE );
                                zmq::message_t empty_msg(0);
                                frontend_.send( empty_msg );
                            }
                            else {
#ifdef DEBUG_OUT
                                std::cerr << "[S] update subscription for " << client << "\n";
#endif
                                it->second.expire_at = now + SUBSCRIBE_TIMEOUT;
                            }
                            break;
                        }
                    case TSubscriptionCommand::DELETE: {
#ifdef DEBUG_OUT
                            std::cerr << "[S] drop subscription for " << client << "\n";
#endif
                            timers_.erase(client);
                            break;
                        }

                    default: break;
                    }

                    // broadcast the subscription
                    zmq::message_t id_copy;
                    id_copy.copy( &id_msg );
                    subscription_.send( id_copy, ZMQ_SNDMORE );
                    cmd.send( subscription_ );
                }
#ifdef DEBUG_OUT
                else {
                    std::cerr << "[S] invalid subscription: " << id_msg << "\n";
                }
#endif
            }

            // backend (from workers) //////////////////////////////////////////
            //
            //  Message structure: [<workerID>] [<DATA>] [<clientID>] .... [<clientID>]
            //
            else if (items[1].revents & ZMQ_POLLIN) {
                zmq::message_t worker_id;
                backend_.recv( &worker_id );

                zmq::message_t data_msg;
                backend_.recv( &data_msg );

                if (data_msg.more()) {
                    zmq::message_t id_msg;
                    do {
                        backend_.recv( &id_msg );

                        zmq::message_t id_copy;
                        id_copy.copy( &id_msg );

                        TClientID id;
                        if (unpack_client_id(id_msg, id)) {
                            auto it = timers_.find( id );
                            if (it == timers_.end()) {
                                // The empty timer record marks a dead subscription.
                                // This item will be removed in expiration checking loop below.
                                timers_[id] = {0, 0};
                                droped_packets_cnt ++;
                            }
                            else
                            // Ignore dead subscribers.
                            if (it->second.expire_at != 0) {
                                routed_packets_cnt ++;
                                zmq::message_t data_copy;
                                data_copy.copy( &data_msg );
                                frontend_.send( id_copy, ZMQ_SNDMORE );
                                frontend_.send( data_copy );
                                it->second.heartbeat_at = now + HEARTBEAT_TIMEOUT - CHECK_INTERVAL;
                            }
                            else
                                droped_packets_cnt ++;
                        }
                    } while (id_msg.more());
                }
            }

            // heartbeat round /////////////////////////////////////////////////
            if (now >= check_at) {
                check_at = s_clock() + CHECK_INTERVAL;
                std::vector<TClientID> expired_ids;
                for (auto it = timers_.begin(); it != timers_.end(); ++it) {
                    if (it->second.expire_at <= now) {
#ifdef DEBUG_OUT
                        std::cerr << "[S] ";
                        if (it->second.expire_at) std::cerr << "expire subscription for: ";
                        else std::cerr << "notify a dead subcription: ";
                        std::cerr << it->first << " at " << now << "\n";
#endif
                        zmq::message_t id_msg(& it->first, sizeof(TClientID));
                        subscription_.send( id_msg, ZMQ_SNDMORE );
                        SubscriptionCommand cmd(TSubscriptionCommand::DELETE);
                        cmd.send( subscription_ );
                        expired_ids.push_back( it->first );
                    }
                    else
                    if (it->second.heartbeat_at <= now) {
                        it->second.heartbeat_at = now + HEARTBEAT_TIMEOUT - CHECK_INTERVAL;
                        zmq::message_t id_msg(& it->first, sizeof(TClientID));
                        frontend_.send( id_msg, ZMQ_SNDMORE );
                        zmq::message_t empty_msg(0);
                        frontend_.send( empty_msg );
                    }
                }
                if (expired_ids.size()) {
#ifdef DEBUG_OUT
                    std::cout << "[S] drop " << expired_ids.size() << " clients: ";
                    for (auto it = expired_ids.begin(); it != expired_ids.end(); ++it ) {
                        if (it != expired_ids.begin()) std::cout << ", ";
                        std::cout << *it;
                    }
                    std::cout << std::endl;
#endif
                    for (auto id : expired_ids) timers_.erase( id );
                }
#ifdef DEBUG_OUT
                std::cout << "[S] routed: " << routed_packets_cnt
                    << " droped: " << droped_packets_cnt
                    << " clients: " << timers_.size()
                    << "\n";
#endif
            }
        }

#ifdef DEBUG_OUT
        std::cout << "[S] interrupted\n";
        for (auto ptr : workers) ptr->terminate();
        for (auto it=worker_threads.begin(); it != worker_threads.end(); ++it) it->join();
        std::cout << "[S] finished\n";
#endif
    }
    catch (std::exception &e) {
        std::cerr << e.what() << std::endl;
    }
    catch (...) {
        std::cerr << "server_task: exception\n";
    }

    try{
        for (auto ptr : workers) {
            delete ptr;
        }
    }
    catch (std::exception &e) {
        std::cerr << e.what() << std::endl;
    }
}

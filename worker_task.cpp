#include "worker_task.h"

#define STAT_INTERVAL   3000

//#define DEBUG_OUT

void worker_task::start () {
    std::string header = "[W" + std::to_string(worker_index_) + "] ";
#ifdef DEBUG_OUT
    std::cout << header << "sender port: " << sender_port_ << "\n";
#endif

    backend_.connect(sender_port_);
    frontend_.connect("inproc://workers");
    subscription_.connect("inproc://subscriptions");

#ifdef DEBUG_OUT
    std::cout << header << "started\n";
#endif
    // some statistics for debugging
    uint64_t    ln_in = 0;
    uint64_t    ln_out = 0;
    uint64_t    subs_upd_cnt = 0;
    uint64_t    subs_del_cnt = 0;
    try {
        // start the subscription
        subscription_.setsockopt(ZMQ_SUBSCRIBE, "", 0);

        zmq::pollitem_t items[] = {
            {subscription_, 0, ZMQ_POLLIN, 0},
            {backend_, 0, ZMQ_POLLIN, 0}
            // we do not expect any messages from the frontend socket
        };

        int64_t print_stat_at = s_clock() + STAT_INTERVAL;
        while (!terminated_) {
            zmq::poll(items, 2, 100);
            int64_t now = s_clock();
            // subscriptions
            if (items[0].revents & ZMQ_POLLIN) {
                zmq::message_t id_msg;
                subscription_.recv( &id_msg );
                TClientID client;
                SubscriptionCommand cmd;

                if (unpack_client_id(id_msg, client) && cmd.recv(subscription_)) {
                    switch (cmd.command) {

                    case TSubscriptionCommand::UPDATE: {
                            subs_upd_cnt ++;
                            auto it = filters_.find(client);
#ifdef DEBUG_OUT
                            if (it == filters_.end())
                                std::cout << header << "subscribe " << client << "\n";
#endif
                            filters_[client] = cmd.filter;
                            break;
                        }

                    case TSubscriptionCommand::DELETE: {
                            subs_del_cnt ++;
                            auto it = filters_.find(client);
                            if (it != filters_.end()) {
#ifdef DEBUG_OUT
                                std::cout << header << "UNsubscribe " << client << "\n";
#endif
                                filters_.erase(it);
                            }
                            break;
                        }

                    default: break;
                    }
                }
            }
            // new data
            else
            if (items[1].revents & ZMQ_POLLIN) {
                ln_in ++;
                zmq::message_t  data_msg;
                backend_.recv( &data_msg );
                std::string line(static_cast<char *>(data_msg.data()), data_msg.size());
                bool data_sent = false;
                TClientID prev;
                for (auto item : filters_) {
                    if (item.second.match( line )) {
                        if (!data_sent) {
                            ln_out ++;
                            zmq::message_t copy_msg;
                            copy_msg.copy( &data_msg );
                            frontend_.send( copy_msg, ZMQ_SNDMORE );
                            data_sent = true;
                            prev = item.first;
                        } else {
                            zmq::message_t id_msg(&prev, sizeof(prev));
                            frontend_.send( id_msg, ZMQ_SNDMORE );
                            prev = item.first;
                        }
                    }
                }
                // sending the last item
                if (data_sent) {
                    zmq::message_t id_msg(&prev, sizeof(prev));
                    frontend_.send( id_msg ); // No SNDMORE flag
                }
            }

            if (now >= print_stat_at) {
                print_stat_at = now + STAT_INTERVAL;
#ifdef DEBUG_OUT
                std::cout << header
                    << "in: " << ln_in
                    << " out: " << ln_out
                    << " upd: " << subs_upd_cnt
                    << " del: " << subs_del_cnt
                    << "\n";
#endif
            }
        }
#ifdef DEBUG_OUT
        std::cout << header << "interrupted\n";
#endif
    }
    catch (std::exception &e) {
        std::cerr << header << e.what() << "\n";
    }
    catch (...) {
        std::cerr << header << "exception\n";
    }
#ifdef DEBUG_OUT
                std::cout << header
                    << "in: " << ln_in
                    << " out: " << ln_out
                    << " upd: " << subs_upd_cnt
                    << " del: " << subs_del_cnt
                    << "\n";
#endif
}

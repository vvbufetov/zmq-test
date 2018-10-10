#ifndef __SUBSCRIPTION_HPP__
#define __SUBSCRIPTION_HPP__

#include <functional>
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <stdexcept>
#include <thread>
#include <memory>
#include <sys/time.h>
#include <time.h>
#include "zmq.hpp"

namespace subscription {

//
//  time related functions
//

typedef int64_t msec_t;

msec_t s_clock (void)
{
    struct timeval tv;
    gettimeofday (&tv, NULL);
    return (msec_t) (tv.tv_sec * 1000 + tv.tv_usec / 1000);
}

void s_sleep (msec_t msecs)
{
    struct timespec t;
    t.tv_sec = msecs / 1000;
    t.tv_nsec = (msecs % 1000) * 1000000;
    nanosleep(&t, nullptr);
}

//
//  structures of parameters
//

struct CommonParams {
    msec_t  subscriptionTimeout = 5000; // milliseconds
    msec_t  heartbeatTimeout = 5000; // milliseconds
    msec_t  deliveryTimeout = 100; // milliseconds
};

struct SenderParams : public CommonParams {
    // the maximun allowed number of active subscriptions
    size_t  maxSubscribers = 0;
    // the size of worker's pool (filtering threads)
    size_t  numberOfWorkers = 2;
};

struct ReceiverParams : public CommonParams {
    // the maximal number of sequential reconnect attempts (zero - no reconnects, -1 - no limits)
    int     maxReconnects = 5;
};

//
//  global constants
//

#define inproc_sender_address "inproc://sender"
#define inproc_workers_address "inproc://workers"
#define inproc_subscription_address "inproc://subscription"

/** subscription commands enumerator
*/
enum class Command : uint8_t {
    UNDEFINED = 0,
    UPDATE = 1,
    DELETE = 2
};

/**
    A wrapper class for the ZMQ node identificator.
    Helps using the ZMQ id as a key in unordered map.

    http://zguide.zeromq.org/php:chapter3
    ZeroMQ v3.0 and later generate a 5 byte identity by default (0 + a random 32bit integer).
*/
struct ZmqIdentity {
    union {
        uint64_t value;
        struct{
            uint8_t v_data[7];
            uint8_t v_size;
        };
    };

    ZmqIdentity ()
        : value(0)
    {}

    ZmqIdentity (uint64_t x)
        : value(x)
    {}

    ZmqIdentity (void * data_, size_t size_)
        : value(0)
    {
        assert(size_ <= sizeof(v_data));
        v_size = size_;
        memcpy(v_data, data_, v_size);
    }

    const void * data () const { return &v_data[0]; }
    size_t size () const { return v_size; }
};

}; // end of namespace subscription

// injecting into `std` namespace support classes for unordered_map
namespace std {

template<>
struct hash<typename subscription::ZmqIdentity> {
    size_t operator() (const subscription::ZmqIdentity& id) const noexcept
    {
        return id.value;
    }
};

template<>
struct equal_to<subscription::ZmqIdentity> {
    bool operator() (const subscription::ZmqIdentity& lhs, const subscription::ZmqIdentity& rhs) const
    {
        return lhs.value == rhs.value;
    }
};

};//namespace std 


namespace subscription {

//
//  send/receive procedures
//

template<class Filter>
struct Subscription {
    Command command;
    Filter  filter;

    Subscription ()
    {}

    Subscription (const Command cmd)
        : command(cmd)
    {}

    Subscription (const Command cmd, const Filter& flt)
        : command(cmd),
          filter(flt)
    {}
};

/** Receives a subscription command packet
    @param socket a reference to the ZMQ socket
    @param data a reference to result object
    @return success status
*/
template<class Filter>
bool recv_packet (zmq::socket_t& socket, Subscription<Filter>& data) {
    zmq::message_t cmd_msg;
    size_t rc = socket.recv(&cmd_msg);
    if (rc <= 0) return false;
    if (cmd_msg.size() != sizeof(data.command)) return false;
    memcpy(&data.command, cmd_msg.data(), cmd_msg.size());

    bool more = data.command == Command::UPDATE;
    if (more && !cmd_msg.more()) return false;
    zmq::message_t flt_msg;
    rc = socket.recv(&flt_msg);
    if (rc <= 0) return false;
    data.filter = Filter(flt_msg.data(), flt_msg.size());
    return true;
}

/** Senda a subscription command packet
    @param socket a reference to the ZMQ socket
    @param data a sended object
    @return success status
*/
template<class Filter>
bool send_packet (zmq::socket_t& socket, Subscription<Filter>& data) {
    zmq::message_t  cmd_msg(&data.command, sizeof(data.command));
    int flags = (data.command == Command::UPDATE) ? ZMQ_SNDMORE : 0;
    bool rc = socket.send(cmd_msg, flags);
    if (!flags)
        return rc;
    zmq::message_t  flt_msg(data.filter.data(), data.filter.size());
    return socket.send(flt_msg);
}

struct Heartbeat {};

/** Sends the empty message as the heartbeat signal.
*/
bool send_packet (zmq::socket_t& socket, Heartbeat&) {
    return socket.send(nullptr,0);
}


/** Receives a packet with identity
*/
template<class I, class D>
bool recv_packet (zmq::socket_t& socket, I& id, D& data) {
    zmq::message_t id_msg;
    size_t rc = socket.recv( &id_msg );
    if (rc <= 0)
        return false;
    if (!id_msg.more())
        return false;
    id = I(id_msg.data(), id_msg.size());
    return recv_packet(socket, data);
}

/** Sends a packet with identity
*/
template<class I, class D>
bool send_packet (zmq::socket_t& socket, I& id, D& data) {
    zmq::message_t id_msg(id.data(), id.size());
    if (!socket.send(id_msg, ZMQ_SNDMORE))
        return false;
    return send_packet(socket, data);
};



/**
    This is an example of Event class.
*/
class StringEvent {
 public:
    StringEvent ()
    {}

    StringEvent (const std::string & text)
        : message(text)
    {}

    // building from a message buffer
    StringEvent (const void* data, const size_t size)
        : message(std::string(static_cast<const char*>(data), size))
    {}

    // data() and size() are used for serialization to zmq::message_t
    const void * data ()
    {
        return message.data();
    }

    size_t size ()
    {
        return message.size();
    }

    std::string& text () { return message; }

 private:
    std::string message;
};


/**
    This is an example of Filter class.
*/
class SubstringFilter {
 public:
    SubstringFilter ()
        : active(false)
    {}

    SubstringFilter (const std::string str)
        : active(true),
          substr(str)
    {}

    // building from a message buffer
    SubstringFilter (const void* data, const size_t size)
        : active(true),
          substr(std::string(static_cast<const char*>(data), size))
    {}

    // data() and size() are used for serialization to zmq::message_t
    const void * data ()
    {
        return substr.data();
    }

    size_t size ()
    {
        return substr.size();
    }

    bool match(StringEvent & ev) {
        std::string text = ev.text();
        return active && (text.find( substr ) != std::string::npos);
    }

 private:
    bool        active;
    std::string substr;
};



template<class Event, class Filter> class ServerTask;

/**
    Runs asynchronous thread for filtering and delivering Event messages.
*/
template<class Event, class Filter>
class Sender {
    typedef ServerTask<Event,Filter> server_task_t;
public:

    /** a constructor
        @param addr an address of subscription listening port
        @param params tuning parameters for the server
    */
    Sender (const std::string & addr, const SenderParams & params = SenderParams())
        : m_zmq_ctx(1),
          m_socket(m_zmq_ctx, ZMQ_PUSH),
          m_server(m_zmq_ctx, addr, params)
    {
        m_socket.bind(inproc_sender_address);
        // start the thread when the socket is binded
        m_server_thread = std::thread(std::bind(&server_task_t::start, &m_server));
    }

    ~Sender ()
    {
        m_socket.setsockopt(ZMQ_LINGER,1);
        m_server.terminate();
        m_server_thread.join();
    }

    /** Sends an event or waits for subscribers.
    */
    void send (Event & event)
    {
        zmq::message_t  message(event.data(), event.size());
        // PUSH socket is blocking
        m_socket.send(message);
    }

    void send (Event && event)
    {
        zmq::message_t  message(event.data(), event.size());
        // PUSH socket is blocking
        m_socket.send(message);
    }

private:
    zmq::context_t  m_zmq_ctx;
    zmq::socket_t   m_socket;
    server_task_t   m_server;
    std::thread     m_server_thread;
};

template<class Event, class Filter> class WorkerTask;

/**
    This class starts worker's threads and routes packets from clients and workers.
*/
template<class Event, class Filter>
class ServerTask {
    struct Timer {
        msec_t  expire_at;
        msec_t  heartbeat_at;
    };
    typedef std::unordered_map<ZmqIdentity,Timer> timers_map_t;
    typedef WorkerTask<Event,Filter> worker_task_t;
    typedef std::vector<worker_task_t> workers_t;

public:
    ServerTask(zmq::context_t& ctx, const std::string listen_port, const SenderParams& params)
        : m_listen_port(listen_port),
          m_backend(ctx, ZMQ_ROUTER),   // connected to workers (recv only)
          m_frontend(ctx, ZMQ_ROUTER),  // connected to clients (send/recv)
          m_subscription(ctx, ZMQ_PUB), // connected to workers (send only)
          m_params(params),
          m_terminated(false)
    {
        for (size_t i = 1; i <= m_params.numberOfWorkers; i++) {
            m_workers.emplace(m_workers.end(), ctx, i);
        }
    }

    void start ();

    void terminate () { m_terminated = true; }

private:
    std::string         m_listen_port;
    zmq::socket_t       m_backend;
    zmq::socket_t       m_frontend;
    zmq::socket_t       m_subscription;
    SenderParams        m_params;
    bool                m_terminated;
    workers_t           m_workers;
    timers_map_t        m_timers;
};

template<class Event, class Filter>
void ServerTask<Event,Filter>::start () {
    try {
        m_backend.bind(inproc_workers_address);
        m_frontend.bind(m_listen_port);
        m_subscription.bind(inproc_subscription_address);

        std::vector<std::thread> worker_threads;
        for (auto it = m_workers.begin(); it != m_workers.end(); ++it) {
            worker_threads.emplace(worker_threads.end(), std::bind(&worker_task_t::start, it));
        }
        zmq::pollitem_t items[] = {
            {m_frontend, 0, ZMQ_POLLIN, 0},
            {m_backend, 0, ZMQ_POLLIN, 0}
        };

        msec_t check_at = s_clock() + m_params.deliveryTimeout;

        while (!m_terminated) {
            // 10 milliseconds
            zmq::poll(items, 2, 10);
            msec_t now = s_clock();

            // frontend (from clients) /////////////////////////////////////////
            if (items[0].revents & ZMQ_POLLIN) {
                ZmqIdentity id;
                Subscription<Filter> msg;
                if (recv_packet(m_frontend, id, msg)) {
                    bool broadcast = true;
                    switch (msg.command) {
                        case Command::UPDATE: {
                            auto it = m_timers.find(id);
                            if (it == m_timers.end()) {
                                if (m_params.maxSubscribers == 0 || m_params.maxSubscribers > m_timers.size()) {
                                    m_timers[id] = {
                                        now + m_params.subscriptionTimeout,
                                        now + m_params.heartbeatTimeout - m_params.deliveryTimeout
                                    };
                                }
                                else
                                    broadcast = false;
                            }
                            else {
                                it->second.expire_at = now + m_params.subscriptionTimeout;
                            }
                            if (broadcast) {
                                // confirm the subscription with a heartbeat
                                Heartbeat h;
                                send_packet(m_frontend, id, h);
                            }
                            break;
                        }
                        case Command::DELETE: {
                            m_timers.erase(id);
                            break;
                        }
                        default: break;
                    }
                    // broadcast to workers
                    if (broadcast)
                        send_packet(m_subscription, id, msg);
                }
            }
            // backend (from workers) //////////////////////////////////////////
            //
            //  Message structure: [<workerID>] [<DATA>] [<clientID>] .... [<clientID>]
            //
            else if (items[1].revents & ZMQ_POLLIN) {
                zmq::message_t worker_id_msg;
                size_t rc = m_backend.recv( &worker_id_msg );
                if (rc < 0) continue;

                zmq::message_t data_msg;
                rc = m_backend.recv( &data_msg );

                if (data_msg.more()) {
                    zmq::message_t id_msg;
                    do {
                        m_backend.recv( &id_msg );
                        ZmqIdentity id(id_msg.data(), id_msg.size());
                        auto it = m_timers.find( id );
                        if (it == m_timers.end()) {
                            // The empty timer record marks a dead subscription.
                            // This item will be removed in expiration checking loop below.
                            m_timers[id] = {0, 0};
                        }
                        else
                        // Ignore dead subscribers.
                        if (it->second.expire_at != 0) {
                            zmq::message_t id_copy;
                            id_copy.copy( &id_msg );
                            zmq::message_t data_copy;
                            data_copy.copy( &data_msg );
                            m_frontend.send( id_copy, ZMQ_SNDMORE );
                            m_frontend.send( data_copy );
                            it->second.heartbeat_at = now + m_params.heartbeatTimeout
                                                          - m_params.deliveryTimeout;
                        }
                    } while (id_msg.more());
                }
            }
            // heartbeat round /////////////////////////////////////////////////
            if (now >= check_at) {
                check_at = s_clock() + m_params.deliveryTimeout;
                for (auto it = m_timers.begin(); it != m_timers.end();) {
                    if (it->second.expire_at <= now) {
                        Subscription<Filter> s(Command::DELETE);
                        send_packet(m_subscription, it->first, s);
                        it = m_timers.erase(it);
                    }
                    else {
                        if (it->second.heartbeat_at <= now) {
                            it->second.heartbeat_at = now + m_params.heartbeatTimeout
                                                          - m_params.deliveryTimeout;
                            Heartbeat hb;
                            send_packet(m_frontend, it->first, hb);
                        }
                        ++ it;
                    }
                }
            }
        }

        for (auto it = m_workers.begin(); it != m_workers.end(); ++it) {
            it->terminate();
        }
        for (auto it = worker_threads.begin(); it != worker_threads.end(); ++it) {
            it->join();
        }
    }
    catch (zmq::error_t &e) {
        std::cerr << "ServerTask terminated due to zmq error: " << e.num() << " " << e.what() << std::endl;
        throw;
    }
    catch (std::exception &e) {
        std::cerr << "ServerTask terminated due to the exception: " << e.what() << std::endl;
        throw;
    }
    catch (...) {
        std::cerr << "ServerTask terminated due to an unknown exception" << std::endl;
        throw;
    }
}

/**
    This class runs a data filtering thread.
*/

template<class Event, class Filter>
class WorkerTask {
    typedef std::unordered_map<ZmqIdentity, Filter> filters_map_t;

 public:
    WorkerTask(zmq::context_t& ctx, const int id)
        : m_worker_id(id),
          m_terminated(false),
          m_backend(ctx, ZMQ_PULL),
          m_frontend(ctx, ZMQ_DEALER),
          m_subscription(ctx, ZMQ_SUB)
    {
        m_frontend.setsockopt(ZMQ_IDENTITY, &m_worker_id, sizeof(m_worker_id));
    }

    void start ();

    void terminate () { m_terminated = true; }

 private:
    int m_worker_id;
    bool m_terminated;
    zmq::socket_t   m_backend;        // to listen to input data
    zmq::socket_t   m_frontend;       // to publish data and heartbeats
    zmq::socket_t   m_subscription;   // to listen to subscription updates
    filters_map_t   m_filters;        // active subscriber's filters
};

template<class Event, class Filter>
void WorkerTask<Event,Filter>::start () {
    try {
        m_backend.connect(inproc_sender_address);
        m_frontend.connect(inproc_workers_address);
        m_subscription.connect(inproc_subscription_address);
        // start the subscription
        m_subscription.setsockopt(ZMQ_SUBSCRIBE, "", 0);

        zmq::pollitem_t items[] = {
            {m_subscription, 0, ZMQ_POLLIN, 0},
            {m_backend, 0, ZMQ_POLLIN, 0}
            // we do not expect any messages from the frontend socket
        };

        while (!m_terminated) {
            zmq::poll(items, 2, 100);
            msec_t now = s_clock();

            // subscriptions
            if (items[0].revents & ZMQ_POLLIN) {
                ZmqIdentity id;
                Subscription<Filter> msg;
                if (recv_packet(m_subscription, id, msg)) {
                    switch (msg.command) {
                        case Command::UPDATE:
                            m_filters[id] = msg.filter;
                            break;
                        case Command::DELETE:
                            m_filters.erase(id);
                            break;
                        default:
                            break;
                    }
                }
            }
            // new data
            else
            if (items[1].revents & ZMQ_POLLIN) {
                zmq::message_t  data_msg;
                size_t rc = m_backend.recv( &data_msg );
                if (rc < 0) continue; // go to while (!m_terminated)
                assert( !data_msg.more() );

                Event event(data_msg.data(), data_msg.size());
                bool data_sent = false;
                ZmqIdentity prev;
                for (auto item : m_filters) {
                    if (item.second.match( event )) {
                        if (!data_sent) {
                            zmq::message_t copy_msg;
                            copy_msg.copy( &data_msg );
                            m_frontend.send( copy_msg, ZMQ_SNDMORE );
                            data_sent = true;
                            prev = item.first;
                        } else {
                            zmq::message_t id_msg(prev.data(), prev.size());
                            m_frontend.send( id_msg, ZMQ_SNDMORE );
                            prev = item.first;
                        }
                    }
                }
                // sending the last item
                if (data_sent) {
                    zmq::message_t id_msg(prev.data(), prev.size());
                    m_frontend.send( id_msg );
                }
            }
        } // end of while (!m_terminated)
    }
    catch (zmq::error_t &e) {
        std::cerr << "WorkerTask terminated due to zmq error: " << e.num() << " " << e.what() << std::endl;
        throw;
    }
    catch (std::exception& e) {
        std::cerr << "WorkerTask terminated due to the exception: " << e.what() << std::endl;
        throw;
    }
    catch (...) {
        std::cerr << "WorkerTask terminated due to an unknown exception" << std::endl;
        throw;
    }
}



/**
    This class realizes client-side protocol of event's subscription.
*/

class NoConnectionError : public std::exception {
public:
    NoConnectionError () {}
};

template<class Event, class Filter>
class Receiver {

    struct Connection {
        zmq::socket_t*  p_socket;
        std::string     address;
        msec_t          expiration_interval;
        msec_t          update_interval;
        msec_t          expires_at;
        msec_t          update_at;
        int             retries;

        Connection (zmq::context_t & ctx, const std::string& address_, const msec_t exp_int, const msec_t upd_int )
            : address(address_),
              expiration_interval(exp_int),
              update_interval(upd_int),
              retries(0)
        {
        }

        ~ Connection ()
        {
            try{
                close();
            }
            catch(...) {}
        }

        void close () {
            if (p_socket != nullptr) {
                p_socket->setsockopt(ZMQ_LINGER, 1);
                delete p_socket;
                p_socket = nullptr;
            }
        }

        void connect (zmq::context_t& ctx, Filter& filter) {
            close();
            p_socket = new zmq::socket_t(ctx, ZMQ_DEALER);
            p_socket->connect(address);
            // TODO: get rid of copiing the filter object
            Subscription<Filter> s(Command::UPDATE, filter);
            send_packet(*p_socket, s);
            msec_t now = s_clock();
            update_at = now + update_interval;
            expires_at = now + expiration_interval;
        }

        void update (const msec_t now, Filter& filter) {
            // TODO: get rid of copiing the filter object
            Subscription<Filter> s(Command::UPDATE, filter);
            send_packet(*p_socket, s);
            update_at = now + update_interval;
        }

        void heartbeat (const msec_t now) {
            expires_at = now + expiration_interval;
            retries = 0;
        }
    };

    typedef std::vector<Connection> connects_t;

public:
    Receiver (const Filter& filter, const ReceiverParams &params)
        : m_ctx(zmq::context_t(1)),
          m_filter(filter),
          m_params(params)
    {}

    void connect (const std::string & address) {
        for (auto it = m_connects.begin(); it != m_connects.end(); ++it) {
            if (it->address == address) return;
        }
        auto it = m_connects.emplace(
            m_connects.end(),
            m_ctx,
            address,
            m_params.heartbeatTimeout, // the connection expiration timeout
            m_params.subscriptionTimeout - m_params.deliveryTimeout // the subscription updating interval
            );
        it->connect(m_ctx, m_filter);
    }

    void disconnect (const std::string & address) {
        for (auto it = m_connects.begin(); it != m_connects.end();) {
            if (it->address == address) {
                it = m_connects.erase(it);
            } else
                ++ it;
        }
    }

    /** Returns an event or wait for it.
        Throws NoConnectionError if there is no connection available or no more reconnection attempts left.
        @return an Event class object
    */
    Event receive () {
        while (true) {
            msec_t now = s_clock();

            // check timers on all connections
            for (auto it = m_connects.begin(); it != m_connects.end();) {
                // 1. disconnect expired connection
                if (it->expires_at < now) {
                    // -1 means "no limits", zero means "no reconnects"
                    if (m_params.maxReconnects < 0 || ++it->retries <= m_params.maxReconnects) {
                        it->connect(m_ctx, m_filter);
                        ++ it;
                    }
                    else
                        it = m_connects.erase(it);
                }
                else {
                    // 2. update subscription (heartbeat)
                    if (it->update_at <= now)
                        it->update(now, m_filter);
                    ++ it;
                }
            }

            if (m_connects.size() == 0)
                throw NoConnectionError();

            zmq::pollitem_t items[m_connects.size()];
            for (size_t i=0; i<m_connects.size(); i++) {
                items[i].socket = (*m_connects[i].p_socket);
                items[i].fd = 0;
                items[i].events = ZMQ_POLLIN;
                items[i].revents = 0;
            }

            // timeout is 10 milliseconds
            int rc = zmq::poll(items, m_connects.size(), 10);
            if (rc == -1) break;
            for (size_t i=0; i<m_connects.size(); i++) {
                if (items[i].revents & ZMQ_POLLIN) {
                    zmq::message_t msg;
                    m_connects[i].p_socket->recv( &msg );
                    m_connects[i].heartbeat(now);
                    if (msg.size() != 0) {
                        return Event(msg.data(), msg.size());
                    }
                }
            }
        }
    }

private:
    zmq::context_t  m_ctx;
    Filter          m_filter;
    ReceiverParams  m_params;
    connects_t      m_connects;
};

}; // namespace subscription

#endif // __SUBSCRIPTION_HPP__

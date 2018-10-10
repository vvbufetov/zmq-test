#include "subscription.hpp"
#include <time.h>
//  This is our client task class.
//  It connects to the server, and then sends a request for subscriptions.
//  It collects responses as they arrive, and it prints them out.

void s_sleep (int msecs)
{
#if (defined (WIN32))
    Sleep (msecs);
#else
    struct timespec t;
    t.tv_sec = msecs / 1000;
    t.tv_nsec = (msecs % 1000) * 1000000;
    nanosleep (&t, NULL);
#endif
}

namespace subs = subscription;
typedef subs::StringEvent Event;
typedef subs::SubstringFilter Filter;

int main (int argc, char ** argv)
{
    if (argc < 3 || argc > 4) {
        std::cerr << "Usage: client \"tcp://host:port\" \"filter_text\" [sleep_time]\n";
        return (-1);
    }
    int64_t sleep_time = 0;
    if (argc == 4) {
        sleep_time = std::stol(argv[3]);
    }

    subs::ReceiverParams p;
    p.maxReconnects = 2;
    subs::Receiver<Event,Filter> receiver(Filter(argv[2]), p);
    receiver.connect(argv[1]);

    while (true) {
            Event ev = receiver.receive();
            std::cout << ev.text() << std::endl;
            if (sleep_time) s_sleep(sleep_time);
    }
    return 0;
}

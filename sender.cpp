/**

    This application reads lines from STDIN and send
    them to all subscribers on a specified port.

*/
#include "subscription.hpp"
#include <iostream>

namespace subs = subscription;
typedef subs::StringEvent Event;
typedef subs::SubstringFilter Filter;
typedef subs::Sender<Event,Filter> Sender;

int main (int argc, char ** argv)
{
    if (argc != 2) {
        std::cerr << "Usage: sender \"tcp://*:port\"\n";
        return (-1);
    }

    subs::SenderParams p;
    p.maxSubscribers = 1;

    Sender s(argv[1], p);
    while (!std::cin.eof()) {
        std::string line;
        std::getline(std::cin, line);
        if (std::cin.eof()) break;
        s.send(Event(line));
    }

    return 0;
}

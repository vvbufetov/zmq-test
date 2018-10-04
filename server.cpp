#include <thread>
#include <stdexcept>
#include "server_task.h"
#include "zhelpers.hpp"

server_task * st = nullptr;

static void server_signal_handler (int signal_value)
{
    std::cerr << "SIGNAL: " << signal_value << "\n";
    if (st) st->terminate();
}

static void server_catch_signals ()
{
    struct sigaction action;
    action.sa_handler = server_signal_handler;
    action.sa_flags = 0;
    sigemptyset (&action.sa_mask);
    sigaction (SIGINT, &action, NULL);
    sigaction (SIGTERM, &action, NULL);
}

int main (int argc, char ** argv)
{
    if (argc < 3 || argc > 4) {
        std::cerr << "Usage: server \"tcp://host:port\" \"tcp://*:port\" [NoW]\n"
            "\tthe first port is used for the sender\n"
            "\tthe second is for clients\n"
            "\tNoW - a number of worker threads\n";
        return (-1);
    }
    size_t NoW = 2;
    if (argc == 4) {
        NoW = std::stol(argv[3]);
        if (NoW <= 0) throw std::runtime_error("Invalid number of threads");
        if (NoW > 100) throw std::runtime_error("Number of threads is too big");
    }

    server_catch_signals();

    st = new server_task(argv[1], argv[2], NoW);
    std::thread t(std::bind(&server_task::start, st));
    t.join();
    delete st;
    st = nullptr;

    return 0;
}

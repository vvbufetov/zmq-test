# because of AC_CONFIG_MACRO_DIR([m4]) in configure.ac
ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = -std=c++11 ${ZMQ_CPPFLAGS} -g -ggdb -Wall -Wextra

AM_LDFLAGS = ${ZMQ_LDFLAGS}

bin_PROGRAMS = sender server client

### sender ###

sender_SOURCES = sender.cpp

sender_LDFLAGS = -static

sender_LDADD = ${ZMQ_LIBS} -lpthread

### server ###

server_SOURCES = server.cpp server_task.cpp worker_task.cpp

server_LDFLAGS = -static

server_LDADD = ${ZMQ_LIBS} -lpthread

### client ###

client_SOURCES = client.cpp

client_LDFLAGS = -static

client_LDADD = ${ZMQ_LIBS} -lpthread

# because of AC_CONFIG_MACRO_DIR([m4]) in configure.ac
ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = -std=c++11 ${ZMQ_CPPFLAGS} -g -ggdb -Wall -Wextra

AM_LDFLAGS = ${ZMQ_LDFLAGS}

bin_PROGRAMS = sender server client

### sender ###

sender_SOURCES = sender.cpp

sender_LDADD = ${ZMQ_LIBS}

### client ###

client_SOURCES = client.cpp

client_LDADD = ${ZMQ_LIBS}

# because of AC_CONFIG_MACRO_DIR([m4]) in configure.ac
ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = -std=c++11 ${ZMQ_CPPFLAGS}

AM_LDFLAGS = ${ZMQ_LDFLAGS}

bin_PROGRAMS = test

### test ###

test_SOURCES = main.cpp

test_LDADD = ${ZMQ_LIBS}

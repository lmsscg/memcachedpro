############################################################################
# MemcachedPro

## Introduction

Memcachedpro is a memcached with persistence

Fork from the memcached-1.4.15

## Features

* intervally save the cache content to disk.

* can specify the number of persitence history.

* the disk space taken will not increase infinitely.

  [the disk space needed] = [memory of memcached] \* [number of persitence history]

* at least one persistence file is valid.

* when the memcached is stopped or crashed,

  the last valid persistence is not consistent with the contents in memory.

* the overhead of persistence is about 25%, according to my test.

## usage

* -w to enable warmup module

* -W to enable persistence module

* -y to specify the persistence interval

## contact

* lmsscg@gmail.com


############################################################################
# Memcached

## Dependencies

* libevent, http://www.monkey.org/~provos/libevent/ (libevent-dev)

## Environment

### Linux

If using Linux, you need a kernel with epoll.  Sure, libevent will
work with normal select, but it sucks.

epoll isn't in Linux 2.4, but there's a backport at:

    http://www.xmailserver.org/linux-patches/nio-improve.html

You want the epoll-lt patch (level-triggered).

### Mac OS X

If you're using MacOS, you'll want libevent 1.1 or higher to deal with
a kqueue bug.

Also, be warned that the -k (mlockall) option to memcached might be
dangerous when using a large cache.  Just make sure the memcached machines
don't swap.  memcached does non-blocking network I/O, but not disk.  (it
should never go to disk, or you've lost the whole point of it)

## Website

* http://www.memcached.org

## Contributing

Want to contribute?  Up-to-date pointers should be at:

* http://contributing.appspot.com/memcached

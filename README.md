cassbouncer
===========

A lightweight connection pooler for Cassandra

Goals:

    1. token aware for reads/writes using describe_ring
    2. lower overhead of describe_keyspace
    3. caching?!
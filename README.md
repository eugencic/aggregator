# PR laboratory No.2

## Aggregator

This is the `aggregator` of the food ordering simulation of the second laboratory work at Network Programming course.
* The `producer` part: https://github.com/eugencic/utm-food-ordering-producer
* The `consumer` part: https://github.com/eugencic/utm-food-ordering-consumer

## Minimum Acceptance Criteria

1. Three web servers which communicates over HTTP protocol between them.2. First web server is producer which produces some data on multiple threads (more than 5) and it sends these data from multiple threds to the second web server.
3. Second server is aggregator which receives and aggregates data from ﬁrst server and populates producer shared resources, a queue or stack with received data.
4. Second server also has multiple threads which extracts one by one elements from producer shared resource and is
sending that extracted data element from second server to the third which is consumer.
5. Third server is consumer, which receives and consumes data from second server and populates shared resources, a queue
or stack with received data.
6. Third server also has multiple threads which extracts one element from shared resource and is sending that extracted data element from it server to the second, aggregator server.
7. Second aggregator server is receiving data from third server and populates consumer shared resources, a queue or stack with received data. In aggregator server you should have 2 shared resources, a producer and consumer a queue or stack with received data.
8. Second server has multiple threads which extracts elements one by one from consumer shared resource and is sending
that extracted data element from second server to the ﬁrst, initial producer server.

## Technologies

* Code editor : Visual Studio Code
* Programming language : Python 3.10

### Clone the repository
```bash
$ git clone https://github.com/eugencic/utm-food-ordering-aggregator
```

### Start the servers

1. Run the `consumer`
2. Run the `aggregator`
3. Run the `producer`
```bash
$ python3 aggregator.py
```
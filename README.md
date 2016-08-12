# Kasocki

Kafka Consumer -> socket.io library.  All messages in Kafka are assumed to be
utf-8 JSON strings.  These are decoded and augmented, and then emitted
to connected socket.io clients.

Supports wildcard topic subscription and arbitrary server side field filtering.

Future features will include subscribing at a partition offsets, and
eventually timestamp based subscriptions.

## TODO

- Use Blizzard/node-rdkafka instead of Wikimedia?
- websocket tests, integration tests.
- return topic, partition offset outside of event/message?
- figure out proper logging
- figure out error responses
- figure out kafka group non assigment
- docker with kafka

- Subscribe at offsets and auto.offset.reset: can't do this for now.
  node-rdkafka doesn't have assign() yet, and timestamp based consumption
  is not even present yet.

- move lib/objectutils.js stuff elsewhere?

-  fix:
```
  node_modules/socket.io-as-promised/node_modules/ascallback/index.js:4
  let p = promise.then(function (ret) {
  ^^^

SyntaxError: Block-scoped declarations (let, const, function, class) not yet supported outside strict mode
```
# Kasocki!


## TODO

- websocket tests?
- return topic, partition offset outside of event/message?
- figure out proper logging
- figure out error responses
- figure out kafka group non assigment
- docker with kafka

- Subscribe at offsets and auto.offset.reset: can't do this for now.
  node-rdkafka doesn't have assign() yet, and timestamp based consumption
  is not even present yet.

- move objectutils.js stuff elsewhere?
- move Kasocki class into its own file and require it from index.js?

-  fix:
```
  node_modules/socket.io-as-promised/node_modules/ascallback/index.js:4
  let p = promise.then(function (ret) {
  ^^^

SyntaxError: Block-scoped declarations (let, const, function, class) not yet supported outside strict mode
```
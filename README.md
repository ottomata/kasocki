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

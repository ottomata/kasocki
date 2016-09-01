[![Travis](https://travis-ci.org/ottomata/kasocki.svg?branch=master)](https://travis-ci.org/ottomata/kasocki)

# Kasocki
_Under heavy development, still a work in progress._

Kafka Consumer -> socket.io library.  All messages in Kafka are assumed to be
utf-8 JSON strings.  These are decoded and augmented, and then emitted
to connected socket.io clients.

Supports wildcard topic subscription and arbitrary server side field filtering.

Future features will include subscribing at a partition offsets, and
eventually timestamp based subscriptions.

## Usage

Server:
```javascript
const server = require('http').createServer();
const io = require('socket.io')(server);
const Kasocki = require('kasocki');

io.on('connection', (socket) => {
    // Create a new Kasocki instance bound to the socket.
    // The socket client can then subscribe to topics,
    // specify filters, and start consuming.
    let kasocki = new Kasocki(socket, kafka_config, bunyan_logger);
});

server.listen(6927);
console.log('Listening for socket.io connections at localhost:6927');
```

Client:
```javascript
var socket = require('socket.io-client')('localhost:6927');

// Print received messages
socket.on('message', function(message){
    console.log('Received: ', message);
});


// Log errors if any received from server
function errorCallback(err) {
    if (err) {
        console.log('Got error: ', err);
    }
}


// Subscribe to some topics
let topics = [
     // subscribe to mytopic
    'mytopic',
    // and to topics that match a regex.
    // Note that this format is a feature of librdkafka.
    // See: https://github.com/edenhill/librdkafka/blob/master/src-cpp/rdkafkacpp.h#L1212
    '^matchme*'
]
socket.emit('subscribe', topics, errorCallback);


// filter for messages based on fields, regexes supported.
let filters = {
    // message.top_level_field must === 1
    'top_level_field': 1,
    // AND message.sub.field must match this regex
    'sub.field': '/^(green|blue).*/'
}
socket.emit('filter', filters, errorCallback);


// start consuming
socket.emit('start', null, errorCallback);


// when finished, disconnect nicely.
socket.disconnect();
```


## TODO

- add documentation about one time subscribe/assign
- Custom Error types?
- return _kafka topic, partition offset outside of event/message?
- docker with kafka
- rdkafka statsd
- logstash?
- Filter for array values


## Blizzard Consume Modes
The Blizzard Kafka client that Kasocki uses has several consume APIs.
Kasocki uses the Standard Non flowing API, and socket.io clients
can choose to receive messages either via the consume socket event
ackCallback, or by emitting a start event, and listening for on 'message'
events to be sent by Kasocki.

Quick testing of each mode:
- Non flowing with ackCb to socket io:     800/s
- Non flowing with on.data emit message:  6000/s  (this is enough for our use case)
- flowing mode with on.data emit: didn't work with lots of messages, but
  perhaps I was doing it wrong.

```
RangeError: Maximum call stack size exceeded
/home/otto/kasocki/node_modules/socket.io/node_modules/engine.io/lib/socket.js:413
      this.sentCallbackFn.push.apply(this.sentCallbackFn, this.packetsFn);
```

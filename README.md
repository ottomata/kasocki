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

- Use blizzard's flowing consumer for start() interface
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

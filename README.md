[![Travis](https://travis-ci.org/ottomata/kasocki.svg?branch=master)](https://travis-ci.org/ottomata/kasocki)

# Kasocki
_Under heavy development, still a work in progress._

Kafka Consumer -> socket.io library.  All messages in Kafka are assumed to be
utf-8 JSON strings.  These are decoded and augmented, and then emitted
to connected socket.io clients.

Supports topic subscription and assignment at partition offsets and server side field filtering.

When Kafka supports it, this will also support timestamp based consumption.

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
    let kasocki = new Kasocki(socket, {
        kafkaConfig: {'metadata.broker.list': 'mybroker:9092'}
    });
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
function ackCallback(err, res) {
    if (err) {
        console.log('Got error: ', err);
    }
    else {
        console.log('ACK from server with', res);
    }
}


// Subscribe to some topics
let topics = [
     // subscribe to mytopic1 and mytopic2
    'mytopic1',
    'mytopic2'
]
socket.emit('subscribe', topics, ackCallback);

// filter for messages based on fields, regexes supported.
let filters = {
    // message.top_level_field must === 1
    'top_level_field': 1,
    // AND message.sub.field must match this regex
    'sub.field': '/^(green|blue).*/'
}
socket.emit('filter', filters, ackCallback);

// start consuming
socket.emit('start', null);

// when finished, disconnect nicely.
socket.disconnect();
```


## TODO

- tests for util.js
- merge util.js and objectutil.js
- look into lodash for some fanciness.
- add documentation about one time subscribe/assign
- Filter for array values
- filter with some other thing?  JSONPath?
- docker with kafka
- rdkafka statsd
- logstash?


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

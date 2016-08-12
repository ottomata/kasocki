'use strict';

/**
 * Test Kasocki socket.io server.
 * Connect to this with a client on port 6927.
 * Kafka broker must be running at localhost:9092.
 */


var server = require('http').createServer();
var io = require('socket.io')(server);

io.on('connection', function(socket){
    const Kasocki = require('./index')(io);
    console.log(socket.id + ' connected');

    // Kafka broker should be running at localhost:9092
    let kasocki = new Kasocki(socket);
});

server.listen(6927);
console.log('Listening for socket.io connections at localhost:6927');

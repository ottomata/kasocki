'use strict';


const http =  require('http');
const socket_io = require('socket.io');
const Kasocki = require('./lib/Kasocki');

/**
 * Kasocki socket.io test server.
 * Connect to this with a client on port 6927.
 * Kafka broker must be running at localhost:9092.
 */
class KasockiServer {

    constructor() {
        this.server = http.createServer();
        this.io = socket_io(this.server);

        this.io.on('connection', (socket) => {
            // Bind Kasocki to this io instance.
            // You could alternatively pass a socket.io namespace.
            console.log(socket.id + ' connected');
            // Kafka broker should be running at localhost:9092
            this.kasocki = new Kasocki(socket);
        });


    }

    listen() {
        this.server.listen(6927);
        console.log('Listening for socket.io connections at localhost:6927');
    }
}

if (require.main === module) {
    new KasockiServer().listen();
}


module.exports = KasockiServer


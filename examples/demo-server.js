#!/usr/bin/env node
'use strict';


const http      = require('http');
const Kasocki   = require('../lib/Kasocki');
const bunyan    = require('bunyan');


const brokers = process.argv[2] || 'localhost:9092'

let log = bunyan.createLogger({
    name: 'KasockiDemoServer',
    level: 'info'
});

let kasockiServer = http.createServer();
let io = require('socket.io')(kasockiServer);


// Create a new Kasocki instance to consume from Kafka
// and send to the newly connected socket.
io.on('connection', (socket) => {
    // Bind Kasocki to this io instance.
    // You could alternatively pass a socket.io namespace.
    log.info(socket.id + ' connected');
    new Kasocki(socket, {
        logger: log,
        kafkaConfig: {
            'metadata.broker.list': brokers
        }
    });
});

kasockiServer.listen(6927);
log.info('Listening for socket.io connections at on port 6927');

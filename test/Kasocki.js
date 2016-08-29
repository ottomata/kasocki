'use strict';


// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

const assert = require('assert');

const http =  require('http');
const socket_io = require('socket.io');
const Kasocki = require('../lib/Kasocki');
const bunyan      = require('bunyan');

/**
 * Kasocki socket.io test server.
 * Connect to this with a client on port 6927.
 * Kafka broker must be running at localhost:9092.
 */
class TestKasockiServer {

    constructor(port, kafkaConfig, allowedTopics) {
        this.port = port;
        this.server = http.createServer();
        this.io = socket_io(this.server);

        this.log = bunyan.createLogger({
            name: 'KasockiTest',
            level: 'warn',
        })

        this.io.on('connection', (socket) => {
            // Kafka broker should be running at localhost:9092.
            // TODO: How to Mock Kafka broker and prep topics and data?
            this.kasocki = new Kasocki(
                socket,
                kafkaConfig,
                allowedTopics,
                // TODO; rearrange when kafkaEventHandlers is finalized
                undefined,
                this.log
            );
        });
    }

    listen() {
        this.server.listen(this.port);
    }

    close() {
        this.kasocki.on_disconnect();
        this.server.close();
    }
}




// function emitCallback(doneCallback, err, res) {
//     console.log('in emit callack');
//     if (err) {
//         throw err;
//     }
//     else {
//         doneCallback();
//     }
// }




describe('Kasocki', function() {
    // this.timeout(5000);

    var serverPort            = 6900;
    const server              = new TestKasockiServer(serverPort);
    var restrictiveServerPort = 6901;
    const restrictiveServer   = new TestKasockiServer(restrictiveServerPort, {}, ['topic1', 'topic2']);

    function createClient(port) {
        return require('socket.io-client')(`http://localhost:${port}/`);
    }


    before(function() {
        server.listen();
        restrictiveServer.listen();
    });

    after(function() {
        server.close();
        restrictiveServer.close();
    });

    it('should subscribe to a single topic', function(done) {
        const client = createClient(serverPort);
        client.on('ready', () => {
            client.emit('subscribe', ['topic1'], (err, res) => {
                if (err)
                    throw err;
                else
                    done();
                client.disconnect();
            });
        });
    });

    it('should subscribe to multiple topics', function(done) {
        const client = createClient(serverPort);
        client.on('ready', () => {
            client.emit('subscribe', ['topic1', 'topic2'], (err, res) => {
                if (err)
                    throw err;
                else
                    done();
                client.disconnect();
            });
        });
    });

    it('should fail subscribe to a single non existent topic', function(done) {
        const client = createClient(serverPort);
        client.on('ready', () => {
            client.emit('subscribe', ['topic0'], (err, res) => {
                // TODO check err type?
                if (err) {
                    done();
                }
                client.disconnect();
            });
        });
    });

    it('should fail subscribe to multiple topics, one of which does not exist', function(done) {
        const client = createClient(serverPort);
        client.on('ready', () => {
            client.emit('subscribe', ['topic1', 'topic0'], (err, res) => {
                // TODO check err type?
                if (err) {
                    done();
                }
                client.disconnect();
            });
        });
    });

    it('should subscribe to a single allowed topic', function(done) {
        const client = createClient(restrictiveServerPort);
        client.on('ready', () => {
            client.emit('subscribe', ['topic1'], (err, res) => {
                if (err)
                    throw err;
                else
                    done();
                client.disconnect();
            });
        });
    });

    it('should subscribe to a multiple allowed topics', function(done) {
        const client = createClient(restrictiveServerPort);
        client.on('ready', () => {
            client.emit('subscribe', ['topic1', 'topic2'], (err, res) => {
                if (err)
                    throw err;
                else
                    done();
                client.disconnect();
            });
        });
    });

    it('should fail subscribe to a single unallowed topic', function(done) {
        const client = createClient(restrictiveServerPort);
        client.on('ready', () => {
            client.emit('subscribe', ['topic3'], (err, res) => {
                // TODO check err type?
                if (err) {
                    done();
                }
                client.disconnect();
            });
        });
    });

    it('should fail subscribe to a multiple topics with at least one not allowed', function(done) {
        const client = createClient(restrictiveServerPort);
        client.on('ready', () => {
            client.emit('subscribe', ['topic1', 'not_allowed'], (err, res) => {
                if (err) {
                    done();
                }
                client.disconnect();
            });
        });
    });

});

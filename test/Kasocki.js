'use strict';


// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

//  NOTE: these tests require a running kafka broker at localhost:9092

const assert       = require('assert');

const http         = require('http');
const socket_io    = require('socket.io');
const Kasocki      = require('../lib/Kasocki');
const bunyan       = require('bunyan');
const P = require('bluebird');


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
            level: 'fatal',
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

    const topicNames = [
        'kasocki_test_01',
        'kasocki_test_02'
    ];

    const serverPort            = 6900;
    const server                = new TestKasockiServer(serverPort);
    const restrictiveServerPort = 6901;
    const restrictiveServer     = new TestKasockiServer(restrictiveServerPort, {}, topicNames);

    function createClient(port) {
        return P.promisifyAll(require('socket.io-client')(`http://localhost:${port}/`));
    }


    before(function() {
        server.listen();
        restrictiveServer.listen();
    });

    after(function() {
        server.close();
        restrictiveServer.close();
    });

    // == Test connect

    it('should connect and return existent topics', function(done) {
        const client = createClient(serverPort);
        client.on('ready', (availableTopics) => {
            // assert that each of these is in available topics returned by
            // client on ready.  We can't be certain about what other topics
            // might exist on our Kafka broker, and without configuring Kasocki
            // with allowedTopics, we will get all topics.  We create
            // these topics in clean_kafka.sh, so we know that at least
            // these should exist and be available.
            ['kasocki_test_01', 'kasocki_test_02', 'kasocki_test_03'].forEach((t) => {
                assert.ok(availableTopics.indexOf(t) >= 0, `${t} not in available topics`);
            });
            done();
            client.disconnect();
        });
    });

    it('should connect and return allowed topics', function(done) {
        const client = createClient(restrictiveServerPort);
        client.on('ready', (availableTopics) => {
            // Only allowedTopics should be returned on ready.
            let shouldBe =  ['kasocki_test_01', 'kasocki_test_02']
            assert.deepEqual(availableTopics, shouldBe, 'ready should return only allowed topics');
            done();
            client.disconnect();
        });
    });

    // TODO: Not sure how to test this.
    // it('should connect and then disconnect since configured allowed topic does not exist', function(done) {
    //     const server = new TestKasockiServer(6092, {}, ['this-topic-does-not-exist']);
    //     const client = createClient(6092);
    //     client.on('disconnect', () => {
    //         console.log("DISCONNECTING");
    //         done();
    //     });
    // });


    // == Test subscribe to latest

    it('should subscribe to a single topic', function(done) {
        const client = createClient(serverPort);
        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', [topicNames[0]])
                .then((res) => {
                    let shouldBe = [ { topic: 'kasocki_test_01', partition: 0, offset: -1 } ]
                    assert.deepEqual(res, shouldBe, 'subscribe');
                    done();
                    client.disconnect();
                });
        });
    });

    it('should subscribe to multiple topics', function(done) {
        const client = createClient(serverPort);
        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', topicNames)
                .then((res) => {
                    let shouldBe = [
                        { topic: 'kasocki_test_01', partition: 0, offset: -1 },
                        { topic: 'kasocki_test_02', partition: 0, offset: -1 }
                    ]
                    assert.deepEqual(res, shouldBe, 'subscribe');
                    done();
                    client.disconnect();
                });
        });
    });

    it('should fail subscribe to a non Array', function(done) {
        const client = createClient(serverPort);
        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', 'not an array')
                .catch((err) => {
                    // TODO check err type?
                    done();
                    client.disconnect();
                });
        });
    });


    it('should fail subscribe to a single non existent topic', function(done) {
        const client = createClient(serverPort);
        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', ['non-existent-topic'])
                .catch((err) => {
                    // TODO check err type?
                    done();
                    client.disconnect();
                });
        });
    });

    it('should fail subscribe to multiple topics, one of which does not exist', function(done) {
        const client = createClient(serverPort);
        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', [topicNames[0], 'non-existent-topic'])
                .catch((err) => {
                    // TODO check err type?
                    done();
                    client.disconnect();
                });
        });
    });

    it('should subscribe to a single allowed topic', function(done) {
        const client = createClient(restrictiveServerPort);
        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', [topicNames[0]])
                .then((res) => {
                    let shouldBe = [ { topic: 'kasocki_test_01', partition: 0, offset: -1 } ]
                    assert.deepEqual(res, shouldBe, 'subscribe');
                    done();
                    client.disconnect();
                });
        });
    });

    it('should subscribe to a multiple allowed topics', function(done) {
        const client = createClient(restrictiveServerPort);
        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', topicNames)
                .then((res) => {
                    let shouldBe = [
                        { topic: 'kasocki_test_01', partition: 0, offset: -1 },
                        { topic: 'kasocki_test_02', partition: 0, offset: -1 }
                    ]
                    assert.deepEqual(res, shouldBe, 'subscribe');
                    done();
                    client.disconnect();
                });
        });
    });

    it('should fail subscribe to a single unallowed topic', function(done) {
        const client = createClient(restrictiveServerPort);
        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', ['non-existent-topic'])
                .catch((err) => {
                    // TODO check err type?
                    done();
                    client.disconnect();
                });
        });
    });

    it('should fail subscribe to a multiple topics with at least one not allowed', function(done) {
        const client = createClient(restrictiveServerPort);
        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', [topicNames[0], 'non-existent-topic'])
                .catch((err) => {
                    // TODO check err type?
                    done();
                    client.disconnect();
                });
        });
    });


    // == Test subscribe with offset

    it('should subscribe with offsets to a single topic', function(done) {
        const client = createClient(serverPort);
        client.on('ready', (availableTopics) => {
            let assignment = [ { topic: topicNames[0], partition: 0, offset: 0 } ];
            client.emitAsync('subscribe', assignment)
                .then((res) => {
                    assert.deepEqual(res, assignment, 'subscribe at offset');
                    done();
                    client.disconnect();
                });
        });
    });

    it('should subscribe with offsets to a single allowwed topic', function(done) {
        const client = createClient(restrictiveServerPort);
        client.on('ready', (availableTopics) => {
            let assignment = [ { topic: topicNames[0], partition: 0, offset: 0 } ];
            client.emitAsync('subscribe', assignment)
                .then((res) => {
                    assert.deepEqual(res, assignment, 'subscribe at offset');
                    done();
                    client.disconnect();
                });
        });
    });

    it('should fail subscribe with offsets to a single not available topic', function(done) {
        const client = createClient(serverPort);
        client.on('ready', (availableTopics) => {
            let assignment = [ { topic: 'not-a-topic', partition: 0, offset: 0 } ];
            client.emitAsync('subscribe', assignment)
                .catch((err) => {
                    // TODO check err type?
                    done();
                    client.disconnect();
                });
        });
    });

    it('should fail subscribe with offsets to a single not allowed topic', function(done) {
        const client = createClient(restrictiveServerPort);
        client.on('ready', (availableTopics) => {
            let assignment = [ { topic: 'kasocki_test_03', partition: 0, offset: 0 } ];
            client.emitAsync('subscribe', assignment)
                .catch((err) => {
                    // TODO check err type?
                    done();
                    client.disconnect();
                });
        });
    });

    it('should subscribe with offsets to a multiple topics', function(done) {
        const client = createClient(serverPort);
        client.on('ready', (availableTopics) => {
            let assignment = [
                { topic: topicNames[0], partition: 0, offset: 0 },
                { topic: topicNames[1], partition: 0, offset: 0 }
            ];
            client.emitAsync('subscribe', assignment)
                .then((res) => {
                    assert.deepEqual(res, assignment, 'subscribe at offset');
                    done();
                    client.disconnect();
                });
        });
    });


    it('should fail subscribe with offsets to a multiple topics where one is not available', function(done) {
        const client = createClient(serverPort);
        client.on('ready', (availableTopics) => {
            let assignment = [
                { topic: topicNames[0], partition: 0, offset: 0 },
                { topic: 'not-a-topic', partition: 0, offset: 0 }
            ];
            client.emitAsync('subscribe', assignment)
                .catch((err) => {
                    // TODO check err type?
                    done();
                    client.disconnect();
                });
        });
    });

    // == Test consume ==

    it('should consume a single message from a single topic', function(done) {
        const client = createClient(serverPort);
        client.on('ready', (availableTopics) => {
            let assignment = [{ topic: topicNames[0], partition: 0, offset: 0 }];
            client.emitAsync('subscribe', assignment)
                .then((subscribedTopics) => {
                    // consume
                    return client.emitAsync('consume', null)
                })
                .then((msg) => {
                    assert.equal(msg._kafka.offset, 0, `check kafka offset in ${topicNames[0]}`);
                    done();
                    client.disconnect();
                });
        })
    });

    it('should consume two messages from a single topic', function(done) {
        const client = createClient(serverPort);
        client.on('ready', () => {
            let assignment = [{ topic: topicNames[1], partition: 0, offset: 0 }];
            client.emitAsync('subscribe', assignment)
            .then((subscribedTopics) => {
                // consume
                return client.emitAsync('consume', null)
            })
            .then((msg) => {
                assert.equal(msg._kafka.offset, 0, `check kafka offset in ${topicNames[0]}`);
                // consume again
                return client.emitAsync('consume', null)
            })
            .then((msg) => {
                assert.equal(msg._kafka.offset, 1, `check kafka offset in ${topicNames[0]}`);
                done();
                client.disconnect();
            });
        });
    });

});

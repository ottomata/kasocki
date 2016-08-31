'use strict';


// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

//  NOTE: these tests require a running kafka broker at localhost:9092

const Kasocki      = require('../lib/Kasocki');

const assert       = require('assert');
const P            = require('bluebird');
const bunyan       = require('bunyan');
const http         = require('http');
const socket_io    = require('socket.io');

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
        this.connectedClients = 0;

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
            this.connectedClients += 1;

            socket.on('disconnect', () => {
                this.connectedClients -= 1;
            })
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


function assertTopicOffsetsInMessages(messages, topicOffsets) {
    topicOffsets.forEach((topicOffset) => {
        let foundIt = messages.find((msg) => {
            return (
                msg._kafka.topic === topicOffset.topic &&
                msg._kafka.offset === topicOffset.offset
            );
        });
        // assert that messages contained a message
        // consumed from topic at offset.
        assert.ok(foundIt, `message in ${topicOffset.topic} at ${topicOffset.partition} should be found`);
    });
}

function assertTopicOffsetsInAssignments(assignments, topicOffsets) {
    topicOffsets.forEach((t) => {
        let foundIt = assignments.find((assigned) => {
            return (
                assigned.topic === t.topic &&
                assigned.partition === t.partition &&
                assigned.offset === t.offset
            );
        });
        assert.ok(
            foundIt,
            `topic ${t.topic} in partition ${t.partition} was subscribed and assigned offset ${t.offset}`
        );
    });
}


describe('Kasocki', function() {
    this.timeout(20000);

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
            client.disconnect();
            done();
        });
    });

    it('should connect and return only allowed topics', function(done) {
        const client = createClient(restrictiveServerPort);
        client.on('ready', (availableTopics) => {
            assert.equal(
                availableTopics.length,
                topicNames.length,
                `Only ${topicNames.length} topics should be available for consumption`
            );
            // Only allowedTopics should be returned on ready.
            topicNames.forEach((t) => {
                let foundIt = availableTopics.find((availableTopic) => {
                    return (availableTopic === t);
                });
                assert.ok(foundIt, `topic ${t} is available for consumption`);
            });
            client.disconnect();
            done();
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
            .then((assignments) => {
                let shouldBe = [ { topic: topicNames[0], partition: 0, offset: -1 } ]
                assert.equal(
                    assignments.length,
                    shouldBe.length,
                    `${shouldBe.length} topic partitions should be assigned`
                );
                assertTopicOffsetsInAssignments(assignments, shouldBe);
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });

    it('should subscribe to multiple topics', function(done) {
        const client = createClient(serverPort);
        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', topicNames)
            .then((assignments) => {
                let shouldBe = [
                    { topic: topicNames[0], partition: 0, offset: -1 },
                    { topic: topicNames[1], partition: 0, offset: -1 }
                ]
                assert.equal(
                    assignments.length,
                    shouldBe.length,
                    `${shouldBe.length} topic partitions should be assigned`
                );
                assertTopicOffsetsInAssignments(assignments, shouldBe);
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });

    it('should fail subscribe to a non Array', function(done) {
        const client = createClient(serverPort);
        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', 'not an array')
            .catch((err) => {
                // TODO check err type?
                assert.ok(true, 'should throw an error');
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });


    it('should fail subscribe to a single non existent topic', function(done) {
        const client = createClient(serverPort);
        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', ['non-existent-topic'])
            .catch((err) => {
                // TODO check err type?
                assert.ok(true, 'should throw an error');
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });

    it('should fail subscribe to multiple topics, one of which does not exist', function(done) {
        const client = createClient(serverPort);
        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', [topicNames[0], 'non-existent-topic'])
            .catch((err) => {
                // TODO check err type?
                assert.ok(true, 'should throw an error');
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });

    it('should subscribe to a single allowed topic', function(done) {
        const client = createClient(restrictiveServerPort);
        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', [topicNames[0]])
            .then((assignments) => {
                let shouldBe = [ { topic: topicNames[0], partition: 0, offset: -1 } ]
                assert.equal(
                    assignments.length,
                    shouldBe.length,
                    `${shouldBe.length} topic partitions should be assigned`
                );
                assertTopicOffsetsInAssignments(assignments, shouldBe);
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });

    it('should subscribe to a multiple allowed topics', function(done) {
        const client = createClient(restrictiveServerPort);
        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', topicNames)
            .then((assignments) => {
                let shouldBe = [
                    { topic: topicNames[0], partition: 0, offset: -1 },
                    { topic: topicNames[1], partition: 0, offset: -1 }
                ]
                assert.equal(
                    assignments.length,
                    shouldBe.length,
                    `${shouldBe.length} topic partitions should be assigned`
                );
                assertTopicOffsetsInAssignments(assignments, shouldBe);
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });

    it('should fail subscribe to a single unallowed topic', function(done) {
        const client = createClient(restrictiveServerPort);
        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', ['non-existent-topic'])
            .catch((err) => {
                // TODO check err type?
                assert.ok(true, 'should throw an error');
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });

    it('should fail subscribe to a multiple topics with at least one not allowed', function(done) {
        const client = createClient(restrictiveServerPort);
        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', [topicNames[0], 'non-existent-topic'])
            .catch((err) => {
                // TODO check err type?
                assert.ok(true, 'should throw an error');
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });


    // == Test subscribe with offset

    it('should subscribe with offsets to a single topic', function(done) {
        const client = createClient(serverPort);
        const assignment = [ { topic: topicNames[0], partition: 0, offset: 0 } ];

        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', assignment)
            .then((returnedAssignment) => {
                assert.equal(
                    returnedAssignment.length,
                    assignment.length,
                    `${assignment.length} topic partitions should be assigned`
                );
                assertTopicOffsetsInAssignments(returnedAssignment, assignment);
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });

    it('should subscribe with offsets to a single allowwed topic', function(done) {
        const client = createClient(restrictiveServerPort);
        const assignment = [ { topic: topicNames[0], partition: 0, offset: 0 } ];

        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', assignment)
            .then((returnedAssignment) => {
                assert.equal(
                    returnedAssignment.length,
                    assignment.length,
                    `${assignment.length} topic partitions should be assigned`
                );
                assertTopicOffsetsInAssignments(returnedAssignment, assignment);
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });

    it('should fail subscribe with offsets to a single not available topic', function(done) {
        const client = createClient(serverPort);
        const assignment = [ { topic: 'not-a-topic', partition: 0, offset: 0 } ];

        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', assignment)
            .catch((err) => {
                // TODO check err type?
                assert.ok(true, 'should throw an error');
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });

    it('should fail subscribe with offsets to a single not allowed topic', function(done) {
        const client = createClient(restrictiveServerPort);
        const assignment = [ { topic: 'kasocki_test_03', partition: 0, offset: 0 } ];

        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', assignment)
            .catch((err) => {
                // TODO check err type?
                assert.ok(true, 'should throw an error');
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });

    it('should subscribe with offsets to a multiple topics', function(done) {
        const client = createClient(serverPort);
        const assignment = [
            { topic: topicNames[0], partition: 0, offset: 0 },
            { topic: topicNames[1], partition: 0, offset: 0 }
        ];

        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', assignment)
            .then((returnedAssignment) => {
                assert.equal(
                    returnedAssignment.length,
                    assignment.length,
                    `${assignment.length} topic partitions should be assigned`
                );
                assertTopicOffsetsInAssignments(returnedAssignment, assignment);
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });


    it('should fail subscribe with offsets to a multiple topics where one is not available', function(done) {
        const client = createClient(serverPort);
        const assignment = [
            { topic: topicNames[0], partition: 0, offset: 0 },
            { topic: 'not-a-topic', partition: 0, offset: 0 }
        ];

        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', assignment)
            .catch((err) => {
                client.disconnect();
                // TODO check err type?
                assert.ok(true, 'should throw an error');
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });

    // == Test consume ==

    it('should consume a single message from a single topic', function(done) {
        const client = createClient(serverPort);
        const assignment = [{ topic: topicNames[0], partition: 0, offset: 0 }];

        client.on('ready', (availableTopics) => {
            client.emitAsync('subscribe', assignment)
            .then((subscribedTopics) => {
                // consume
                return client.emitAsync('consume', null)
            })
            .then((msg) => {
                assert.equal(msg._kafka.offset, 0, `check kafka offset in ${topicNames[0]}`);
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        })
    });

    it('should consume two messages from a single topic', function(done) {
        const client = createClient(serverPort);
        const assignment = [{ topic: topicNames[1], partition: 0, offset: 0 }];

        client.on('ready', () => {
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
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });

    it('should consume three messages from two topics', function(done) {
        const client = createClient(serverPort);
        client.on('ready', () => {
            const assignment = [
                { topic: topicNames[0], partition: 0, offset: 0 },
                { topic: topicNames[1], partition: 0, offset: 0 }
            ];
            client.emitAsync('subscribe', assignment)
            .then((subscribedTopics) => {
                // Consume three messages
                return Promise.all([
                    client.emitAsync('consume', null),
                    client.emitAsync('consume', null),
                    client.emitAsync('consume', null)
                ]);
            })
            .then((messages) => {
                // Look for each of the following topic and offsets
                // to have been consumed.
                let shouldHave = [
                    { topic: topicNames[0], offset: 0 },
                    { topic: topicNames[1], offset: 0 },
                    { topic: topicNames[1], offset: 1 }
                ]
                assert.equal(messages.length, shouldHave.length, `should have consumed ${shouldHave.length} messages`);
                assertTopicOffsetsInMessages(messages, shouldHave);
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });


    // == Test filter

    it('should consume two messages from two topics with a simple filter', function(done) {
        const client = createClient(serverPort);

        const assignment = [
            { topic: topicNames[0], partition: 0, offset: 0 },
            { topic: topicNames[1], partition: 0, offset: 0 }
        ];

        // Filter where price is 25.00
        const filters = {
            'price': 25.00
        }

        client.on('ready', () => {

            client.emitAsync('subscribe', assignment)
            .then((subscribedTopics) => {
                return client.emitAsync('filter', filters)
            })
            .then(() => {
                // Consume two messages
                return Promise.all([
                    client.emitAsync('consume', null),
                    client.emitAsync('consume', null)
                ]);
            })
            .then((messages) => {
                // Look for each of the following topic and offsets
                // to have been consumed.
                let shouldHave = [
                    { topic: topicNames[0], offset: 0 },
                    { topic: topicNames[1], offset: 1 }
                ]
                assert.equal(messages.length, shouldHave.length, `should have consumed ${shouldHave.length} messages`);
                assertTopicOffsetsInMessages(messages, shouldHave);
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });

    it('should consume two messages from two topics with a dotted filter', function(done) {
        const client = createClient(serverPort);

        const assignment = [
            { topic: topicNames[0], partition: 0, offset: 0 },
            { topic: topicNames[1], partition: 0, offset: 0 }
        ];

        // Filter where user.last_name is Berry
        const filters = {
            'user.last_name': 'Berry'
        }

        client.on('ready', () => {
            client.emitAsync('subscribe', assignment)
            .then((subscribedTopics) => {
                return client.emitAsync('filter', filters)
            })
            .then((r) => {
                // Consume two messages
                return Promise.all([
                    client.emitAsync('consume', null),
                    client.emitAsync('consume', null)
                ]);
            })
            .then((messages) => {
                // Look for each of the following topic and offsets
                // to have been consumed.
                let shouldHave = [
                    { topic: topicNames[0], offset: 0 },
                    { topic: topicNames[1], offset: 0 }
                ]
                assert.equal(messages.length, shouldHave.length, `should have consumed ${shouldHave.length} messages`);
                assertTopicOffsetsInMessages(messages, shouldHave);
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });

    it('should consume two messages from two topics with a regex filter', function(done) {
        const client = createClient(serverPort);

        const assignment = [
            { topic: topicNames[0], partition: 0, offset: 0 },
            { topic: topicNames[1], partition: 0, offset: 0 }
        ];

        // Filter where name matches a regex
        const filters = {
            'name': '/(green|red) doors?$/'
        }

        client.on('ready', () => {
            client.emitAsync('subscribe', assignment)
            .then((subscribedTopics) => {
                return client.emitAsync('filter', filters)
            })
            .then(() => {
                // Consume two messages
                return Promise.all([
                    client.emitAsync('consume', null),
                    client.emitAsync('consume', null)
                ]);
            })
            .then((messages) => {
                // Look for each of the following topic and offsets
                // to have been consumed.
                let shouldHave = [
                    { topic: topicNames[1], offset: 0 },
                    { topic: topicNames[1], offset: 1 }
                ]
                assert.equal(messages.length, shouldHave.length, `should have consumed ${shouldHave.length} messages`);
                assertTopicOffsetsInMessages(messages, shouldHave);
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });

    it('should consume one message from two topics with a dotted and a regex filter', function(done) {
        const client = createClient(serverPort);

        const assignment = [
            { topic: topicNames[0], partition: 0, offset: 0 },
            { topic: topicNames[1], partition: 0, offset: 0 }
        ];

        // Filter where user.last_name is Berry and name matches a regex
        const filters = {
            'user.last_name': 'Berry',
            'name': '/(green|red) doors?$/'
        }

        client.on('ready', () => {
            client.emitAsync('subscribe', assignment)
            .then((subscribedTopics) => {
                return client.emitAsync('filter', filters)
            })
            .then(() => {
                // consume one message
                return client.emitAsync('consume', null)
            })
            .then((msg) => {
                // Look for each of the following topic and offsets
                // to have been consumed.
                let shouldHave = [
                    { topic: topicNames[1], offset: 0 },
                ]
                assertTopicOffsetsInMessages([msg], shouldHave);
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });


    // == Test push based consume with start

    it('should handle three messages from two topics', function(done) {
        const client = createClient(serverPort);

        const assignment = [
            { topic: topicNames[0], partition: 0, offset: 0 },
            { topic: topicNames[1], partition: 0, offset: 0 }
        ];

        // Collect messages
        var messages = [];
        client.on('message', (msg) => {
            messages.push(msg);
        });

        client.on('ready', () => {
            client.emitAsync('subscribe', assignment)
            .then((subscribedTopics) => {
                // start consuming, the on message handler will collect them
                return client.emitAsync('start', null);
            })
            // wait 3 seconds to finish getting messages
            .delay(3000)
            .then(() => {
                // Look for each of the following topic and offsets
                // to have been consumed.
                let shouldHave = [
                    { topic: topicNames[0], offset: 0 },
                    { topic: topicNames[1], offset: 0 },
                    { topic: topicNames[1], offset: 1 }
                ]
                assert.equal(messages.length, shouldHave.length, `should have consumed ${shouldHave.length} messages`);
                assertTopicOffsetsInMessages(messages, shouldHave);
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });


    it('should handle one message from two topics with a dotted and a regex filter', function(done) {
        const client = createClient(serverPort);

        const assignment = [
            { topic: topicNames[0], partition: 0, offset: 0 },
            { topic: topicNames[1], partition: 0, offset: 0 }
        ];

        // Filter where user.last_name is Berry and name matches a regex
        const filters = {
            'user.last_name': 'Berry',
            'name': '/(green|red) doors?$/'
        }

        // Collect messages
        var messages = [];
        client.on('message', (msg) => {
            messages.push(msg);
        });

        client.on('ready', () => {
            client.emitAsync('subscribe', assignment)
            .then((subscribedTopics) => {
                return client.emitAsync('filter', filters)
            })
            .then(() => {
                // start consuming, the on message handler will collect them
                return client.emitAsync('start', null);
            })
            // wait 3 seconds to finish getting messages
            .delay(3000)
            .then(() => {
                // Look for each of the following topic and offsets
                // to have been consumed.
                let shouldHave = [
                    { topic: topicNames[1], offset: 0 },
                ]
                assert.equal(messages.length, shouldHave.length, `should have consumed ${shouldHave.length} messages`);
                assertTopicOffsetsInMessages(messages, shouldHave);
            })
            .finally(() => {
                client.disconnect();
                done();
            });
        });
    });

});

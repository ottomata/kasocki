'use strict';


// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

var assert = require('assert');

var utils = require('../lib/utils.js');

describe('buildMessageAsync', () => {
    it('should return an augmented message from a Kafka message', () => {
        let kafkaMessage = {
            message: '{ "first_name": "Dorkus", "last_name": "Berry" }',
            topic: 'test',
            partition: 1,
            offset: 123,
            key: 'myKey',
        };

        utils.buildMessageAsync(kafkaMessage)
        .then((msg) => {
            assert.equal(msg.meta.topic, kafkaMessage.topic, 'built message should have topic');
            assert.equal(msg.meta.partition, kafkaMessage.partition, 'built message should have partition');
            assert.equal(msg.meta.offset, kafkaMessage.offset, 'built message should have offset');
            assert.equal(msg.meta.key, kafkaMessage.key, 'built message should have key');
        });
    });
});



//  TODO: other utils.js unit tests

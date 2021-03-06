'use strict';

/**
 * Collection of utility functions for Kasocki.
 */

const objectutils         = require('./objectutils');

const errors              = require('./error.js');
const InvalidMessageError = errors.InvalidMessageError;

const kafka               = require('node-rdkafka');
const Promise             = require('bluebird');



/**
 * Returns a Promise of a promisified and connected rdkafka.KafkaConsumer.
 *
 * @param  {Object} kafkaConfig
 * @param  {Object} topicConfig
 * @return {Promise<KafkaConsumer>} Promisified KafkaConsumer
 */
function createKafkaConsumerAsync(kafkaConfig, topicConfig) {
    topicConfig = topicConfig || {}

    const consumer = Promise.promisifyAll(
        new kafka.KafkaConsumer(kafkaConfig, topicConfig)
    );

    return consumer.connectAsync(undefined)
    .then((metadata) => {
        return consumer;
    });
}


/**
 * Return the intersection of existent topics and allowedTopics,
 * or just all existent topics if allowedTopics is undefined.
 *
 * @param  {KafkaConsumer} connected kafkaConsumer
 * @param  {Array}         allowedTopics
 * @return {Array}         available topics
 */
function getAvailableTopics(kafkaConsumer, allowedTopics) {
    const existentTopics = kafkaConsumer._metadata.topics.map(
        e => e.name
    )
    .filter(t => t !== '__consumer_offsets');

    // TODO: throw error if allowed topic does not currently exist
    if (allowedTopics) {
        return existentTopics.filter(
            t => allowedTopics.indexOf(t) >= 0
        );
    }
    else {
        return existentTopics;
    }
}


/**
 * Given an Array of topics, this will return an array of
 * [{topic: t1, partition: 0, offset: -1}, ...]
 * for each topic-partition.  This is useful for manually passing
 * an to KafkaConsumer.assign, without actually subscribing
 * a consumer group with Kafka.
 *
 * TODO: add docs about what consumer._metadata.topics looks like and how
 * this builds assignemnts.
 *
 * @param  {Array} topics
 * @return {Array} TopicPartition assignments starting at latest offset.
 */
function buildAssignments(kafkaConsumer, topics) {
    // Find the topic metadata
    return kafkaConsumer._metadata.topics.filter((t) => {
        return topics.indexOf(t.name) >= 0;
    })
    // Map them into topic, partition, offset: -1 (latest) assignment.
    .map((t) => {
        return t.partitions.map((p) => {
            return {topic: t.name, partition: p.id, offset: -1};
        });
    })
    // Flatten
    .reduce((a, b) => a.concat(b));
}


/**
 * Parses kafkaMessage.message as a JSON string and then
 * augments the object with kafka message metadata.
 * Returns this as a Promise.
 *
 * @param  {KafkaMesssage} kafkaMessage
 * @return {Promise<Object>}
 *
 */
function buildMessageAsync(kafkaMessage) {
    return new Promise((resolve, reject) => {
        let message = objectutils.factory(kafkaMessage.message);

        // TODO: rename this?
        if (!('meta' in message)) {
            message.meta = {}
        }

        // TODO: should these be flat in the event meta like this?
        // Maybe this should be message.meta._kafka? message.meta._transport?
        message.meta.topic     = kafkaMessage.topic;
        message.meta.partition = kafkaMessage.partition;
        message.meta.offset    = kafkaMessage.offset;
        message.meta.key       = kafkaMessage.key;

        resolve(message);
    })
    .catch((e) => {
        // TODO: different error type?
        throw new InvalidMessageError(
            'Failed building message from Kafka in ' +
            `${kafkaMessage.topic}-${kafkaMessage.partition} ` +
            `at offset ${kafkaMessage.offset}. ` +
            `'${kafkaMessage.message.toString()}': ${e}`
        );
    });
}




module.exports = {
    createKafkaConsumerAsync:   createKafkaConsumerAsync,
    getAvailableTopics:         getAvailableTopics,
    buildAssignments:           buildAssignments,
    buildMessageAsync:          buildMessageAsync,
}

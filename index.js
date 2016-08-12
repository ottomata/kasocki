'use strict';

const objectutils = require('./objectutils');

const kafka = require('rdkafka');
const socketAsPromised = require('socket.io-as-promised');
const serializerr = require('serializerr');

//  TODO: state machine for states and flow.
//  subscribe -> start -> pause -> stop, etc.


function __debugKafkaMessage(msg) {
    var util = require("util");
    msg.payloadString = msg.payload.toString();
    console.log(util.inspect(msg, false, null));
}


//  TODO: Should this wrap the message.payload json object?
//  Should it place the topic, partition, offset field
//  into message?
function buildEvent(kafkaMessage) {
    try {
        let event = objectutils.factory(kafkaMessage.payload);
        // TODO: rename this?
        event._kafka = {
            'topic': kafkaMessage.topicName,
            'partition': kafkaMessage.partition,
            'offset': kafkaMessage.offset,
            'key': kafkaMessage.key
        }
        return event;
    } catch (e) {
        console.log('Failed to decode: %o, %o', message.payload.toString(), e);
        return undefined;
    }
}


/**
 * Represents a Kafka Consumer -> socket.io connection.
 *
 * This creates a new Kafka Consumer and passes consumed
 * messages to the connected socket.io client.
 *
 * Socket Handlers:
 * Any method that starts with 'on_' will be registered as a handler
 * for that socket event.  That is an on_subscribe function will be registered
 * as the handler for the 'subscribe' event.
 *
 * This module uses socket.io-as-promised.  As such, every socket.on
 * handler needs to return a Promise.  If the Promise is rejected,
 * the handleSocketError function will be invoked.  This function
 * handles serialization of Errors via serializerr.
 * In addition to that error handler, each auto registered socket.on handler
 * will catch rejected promises and call this._error() with them.
 * This allows the errors to be logged and augmented before they are handed
 * off to the handleSocketError function for serialization.
 */
class Kasocki {
    /**
     * @param {socket.io Object} socket
     * @param {Object} kafkaConfig suitable for passing to rdkafka.KafkaConsumer
     *                 constructor.  group.id and enable.auto.commit cannot
     *                 be provided and will be overridden.
     */
    constructor(
        socket,
        kafkaConfig,
        logger
    ) {
        this.socket  = socket;
        this.name    = this.socket.id
        this.filters = null;

        this.running = false;
        this.closing = false;

        // If we are given a logger, assume it is a bunyan logger
        // and create a child.
        if (logger) {
            this.logger = logger.child({
                'name': 'kasocki',
                'socket': this.name
            });
            // Make this.log a function.
            this.log = this.logger.log.bind(this.logger);
        }
        // Else use debug logging.
        else {
            // TODO: just use our own bunyon logger here, might be better.
            const debug = require('debug');
            debug.enable('kasocki:' + this.name);
            this.log = debug('kasocki:' + this.name);
        }

        this.log('info', 'Creating new Kasocki instance ' + this.name);

        // Default kafkaConfigs to use if not provided in kafkaConfig.
        // TODO: tune these.
        const defaultKafkaConfig = {
            'metadata.broker.list': 'localhost:9092',
        }

        // These configs MUST be set for a Kasocki KafkaConsumer.
        // The are not overridable.
        // We want to avoid making Kafka manage consumer info
        // for websocket clients.
        //   A. no offset commits
        //   B. no consumer group management/balancing.
        // A. is achieved simply by setting enable.auto.commit: false.
        // B. is more complicated.  Until assign() is supported by
        // node-rdkafka, and/or
        // https://github.com/edenhill/librdkafka/issues/593 is resolved,
        // there is no way to keep Kafka from managing clients.  So,
        // we fake it by using the socket name, which will be unique
        // for each socket instance.
        const mandatoryKafkaConfig = {
            'enable.auto.commit': false,
            'group.id': this.name,
        }

        // Merge provided over default configs.
        kafkaConfig = Object.assign(defaultKafkaConfig, kafkaConfig);
        // Merge mandatory over provided configs.
        kafkaConfig = Object.assign(kafkaConfig, mandatoryKafkaConfig);

        // Create our kafka consumer instance.
        this.kafkaConsumer = new kafka.KafkaConsumer(kafkaConfig);

        // Register methods that start with on_ as socket event handlers.
        this._registerHandlers();
    }


    /**
     * Returns all method names that start with 'on_'.
     */
    _getHandlers() {
        return Object.getOwnPropertyNames(Object.getPrototypeOf(this))
        // filter for all functions that start with on_
        .filter((p) => {
            return p.substring(0, 3) == 'on_' && typeof(this[p]) == 'function';
        });
    }


    /**
     * Loops through each method name that starts with 'on_' and
     * registers it as a socket event handler for that event.
     * E.g. 'on_subscribe' => this.subscribe
     */
    _registerHandlers() {
        // Make bound handleError function that
        // we can use to automatically log
        // and return serialized errors to clients.
        let handleError = this._error.bind(this);

        // Loop through each of this' prototypes own properties and
        // register a socket event handler for any method that starts
        // with 'on_'.
        this._getHandlers().forEach((handler) => {
            let event = handler.substring(3);
            this.log('debug', `Registering socket event '${event}' to be handled by ${handler}`);

            // Register event to be handled by this[handler].
            // Any errors will be caught and converted into
            // a rejected Promise by handleError.
            this.socket.on(event, (arg) => {
                // TODO: should this be wrapped in a try/catch
                // to catch and Promise.reject all errors, even those
                // that are not rejected Promises generated by this class?
                return this[handler](arg).catch(handleError);
            });
        });
    }


    /**
     * Subscribes to Kafka topics.  If the consumer is already
     * subscribed and started, this will end up unsubscribing
     * the previously subscribed topics.
     * TODO: documentation about regexes.  See:
     * https://github.com/edenhill/librdkafka/blob/master/src-cpp/rdkafkacpp.h#L1212
     * // TODO: provide starting offsets, maybe offset reset.
     * @param {Array} topics
     */
    on_subscribe(topics) {
        if (this.closing) {
            return Promise.reject('Cannot subscribe, already closing');
        }

        this.log('info', 'Subscribing to topics.', {'topics': topics})
        this.kafkaConsumer.subscribe(topics);
        return Promise.resolve();
    }


    /**
     * Iterates through filters.  If any filter looks like
     * a regex, then this will convert the string into a RegExp
     * and save it over the string.  this.filters will be
     * replaced by the list of filters given, and will be suitable
     * for passing to objectutils.match().
     */
    on_filter(filters) {

        for (const key in filters) {
            let filter = filters[key]

            // Filters must be either strings or numbers.
            if (typeof(filter) == 'object') {
                return Promise.reject(
                    `Invalid filter for ${key}, cannot filter using an object.`
                );
            }

            // If this filter is a regex, it will begin and end with /
            if (typeof(filter) == 'string' &&
                filter.charAt(0) === '/'   &&
                filter.charAt(filter.length - 1) === '/'
            ) {
                // Convert this to a regex and save it in filters.
                this.log('debug', `Converting ${filter} to RegExp.`);
                try {
                    filters[key] = new RegExp(filter.substring(1, filter.length - 1));
                }
                catch(e) {
                    return Promise.reject(`Failed converting filter to RegExp: ${e.message}`);
                }
            }
        }
        // TODO: why doesn't filters show up in app child logger?
        this.log('info', 'Now filtering.', filters);
        this.filters = filters;
        return Promise.resolve();
    }


    /**
     * Starts the consume loop.
     */
    on_start() {
        // Already running
        if (this.running) {
            return Promise.reject('Cannot start, already started.');
        }
        else if (this.closing) {
            return Promise.reject('Cannot start, already closing.');
        }
        else {
            this.running = true;

            // Loop until not this.running or until error.
            this.log('info', 'Starting');
            return this._loop();
        }
    }


    /**
     * Pauses the consume loop.  Does nothing
     * if not this.running.
     */
    on_pause() {
        this.log('info', "Pausing.");
        this.running = false;
        return Promise.resolve();
    }


    /**
     * Stops the consume loop, and closes the kafkaConsumer,
     * and disconnects the websocket.
     */
    on_disconnect() {
        this.log('info', 'Closing kafka consumer and disconnecting websocket.');
        this.running = false;
        this.closing = true;

        if ('kafkaConsumer' in this) {
            this.kafkaConsumer.close();
            delete this.kafkaConsumer;
        }

        // TODO: do we need this?  Probably if we want to disconnect
        // the socket on error.
        this.socket.disconnect(true);
        return Promise.resolve();
    }


    /**
     * Consumes 1 message from Kafka and emits it to the socket.
     * The kafkaConsumer will throw an error if it is not yet
     * subscribed to any topics.
     *
     * TODO: Not sure if we want to allow single consume calls.  This
     * might not be a handler in the future.
     */
    on_consume() {
        // Consume a message from Kafka
        return this.kafkaConsumer.consume()
            // Then convert it to an object and emit it to the socket
            .then((kafkaMessage) => {
                // TODO get event async?
                const event = buildEvent(kafkaMessage);

                if (event) {
                    if (this._match(event)) {
                        this.socket.emit('message', event)
                    }
                }

                // If we get this far, no error was thrown,
                // so return resolved Promise.
                return Promise.resolve();
            });
    }


    /**
     * While this.running, calls consumeAndEmit() in a loop.
     */
    _loop() {
        if (!this.running) {
            this.log('debug', 'Consume loop stopping.');
            return Promise.resolve();
        }

        return this.on_consume()
            // next iteration of consume loop
            .then(this._loop.bind(this));
    }


    _match(obj) {
        if (!this.filters) {
            return true;
        }
        else {
            // TODO: async filtering?
            return objectutils.match(obj, this.filters);
        }
    }


    /**
     * Creates a new Error object and returns
     * a rejected Promise with the new Error object
     * as its value.  This also logs the message
     * at error level.
     */
    _error(err) {

        if (!(err instanceof Error)) {
            err = new Error(err);
        }
        this.log('error', err.message);

        err.socket = this.name;

        // Returning a rejected promise from a socket.on handler
        // with socket.io-as-promised will cause the handleSocketError
        // function to be called, which will serialize this err
        // and send it to the client.
        // TODO: Should we close the socket on errors?
        // this.close();
        return Promise.reject(err);
    }

}

/**
 * Promise error handler for socket.io-as-promised.  This serializes
 * any object (including Errors) using serializerr.  The stack
 * will be removed, and the socket.io event name will be added
 * to the error's properties.
 *
 * @return rejected Promise.  If you return this from a
 *         socketAsPromised socket event handler, the serialized
 *         Error will be given to the client.
 */
function handleSocketError(err, socketEvent) {
    // debug.enable('handler');
    // debug('handler')(err);
    // logger.log('error', err);
    err.event = socketEvent;
    delete err.stack;
    return Promise.reject(serializerr(err));
}


module.exports = function(io) {
    // Need to pass a socket.io io or namespace object
    // to this module so that we can make it use socket.io-as-promised
    // with an error handler.

    io.use(socketAsPromised({handleError: handleSocketError}));
    return Kasocki;
}
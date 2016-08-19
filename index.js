'use strict';

/**
 * This module needs to be required with a socket.io io or namespace
 * object. The io object will then be configured to use
 * socket.io-as-promised.
 *
 * Usage:
 * const Kasocki = require('kasocki');
 * io.on('connection', (socket) => {
 *     let kasocki = new Kasocki(socket, kafka_config, bunyan_logger);
 * });
 */


const objectutils      = require('./lib/objectutils');
const kafka            = require('node-rdkafka');
const serializerr      = require('serializerr');
const P = require('bluebird');

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

        // TODO: accept topicConfig
        const topicConfig = {};

        // Create our kafka consumer instance.  This will
        // set this.kafkaConsumer once it has connected.
        this.createKafkaConsumer(kafkaConfig, topicConfig)

        // Register methods that start with on_ as socket event handlers.
        .then(this._registerHandlers.bind(this))

        // KafkaConsumer has connected, and handlers are registered,
        // tell the client we are ready.
        .then(() => {
            this.socket.emit('ready');
        })

        // If anything bad during initialization, disconnect now.
        .catch((e) => {
            this.log('error', `Failed Kasocki initialization: ${e.toString()}`);
            this.on_disconnect();
        });
    }

    /**
     * Creates a new promisified and connected this.kafkaConsumer.
     * Returns a Promise that when resolved will have set
     * this.kafkaConsumer to a connected promisified kafka.KafkaConsumer
     * instance.
     */
    createKafkaConsumer(kafkaConfig, topicConfig) {
        const consumer = P.promisifyAll(
            new kafka.KafkaConsumer(kafkaConfig, topicConfig)
        );
        return consumer.connectAsync(undefined)
        .then((metadata) => {
            // TODO debug log metadata?
            this.kafkaConsumer = consumer;
        })
    }

    /**
     * Returns all method names that start with 'on_'.
     */
    _getHandlers() {
        return Object.getOwnPropertyNames(Object.getPrototypeOf(this))
        // filter for all functions that start with on_
        .filter((p) => {
            return p.substring(0, 3) === 'on_' && typeof this[p] === 'function';
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
            const socketEvent = handler.substring(3);
            this.log('debug', `Registering socket event '${socketEvent}' to be handled by ${handler}`);

            // Register socket event to be handled by this[handler].
            this.socket.on(
                socketEvent,
                // Wrap this[handler] with the _handlerWrapper function.
                // This wraps it with error handling and calls
                // the socket callback appropriately.
                this.constructor._handlerWrapper.bind(
                    this,
                    this[handler].bind(this),
                    socketEvent
                )
            );
        });
    }


    /**
     * Wraps handlerFunction with error handling, logging, and
     * auto calling of the socket done callback appropriately.
     * If handlerFunction throws or returns an Error, it will
     * be augmented and serialized and then given to the client's
     * emit callback as the first argument.  The result will be
     * given to the client's emit callback as the second argument.
     * If result is returned from handlerFunction as a Promise,
     * it will be resolved before cb is called.
     * If cb is not defined (e.g. during a disconnect event)
     * it will not be called.
     *
     * @param {function} handlerFunction a on_ function that will be
     *                   called as a socket.on handler.
     * @param {string}   socketEvent the name of the socket event
     *                   that this handlerFunction will be called for.
     * @param {*}        arg emitted from the client to pass to handlerFunction
     * @param {function} cb socket.io done callback.  If given, this will be
     *                   invoked as cb(error, result).  If no error happened,
     *                   then error will be undefined.
     * @return Promise   although this probably won't be used.
     */
    static _handlerWrapper(handlerFunction, socketEvent, arg, cb) {
        // Wrap handlerFunction in a Promise.  This makes
        // error via .catch really easy, and also makes
        // returning a Promise from a handlerFunction work.
        return new Promise((resolve, reject) => {
            const result = handlerFunction(arg);

            // Reject if we are returned an error.
            if (result instanceof Error) {
                reject(result);
            }
            resolve(result);
        })

        // Cool, everything went well, call cb with the result value.
        .then((v) => {
            if (cb) {
                cb(undefined, v);
            }
        })

        // We either rejected or were thrown an Error.
        // Call cb with the augmented and serialized error.
        .catch((e) => {
            e = this._error(e, {'socketEvent': socketEvent});
            if (cb) {
                cb(e, undefined);
            }
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
            this.log('warn', 'Cannot subscribe, already closing.');
            return;
        }

        this.kafkaConsumer.unsubscribe();
        this.log('info', 'Subscribing to topics.', {'topics': topics})
        this.kafkaConsumer.subscribe(topics);
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
            const filter = filters[key]

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
                    throw new Error(`Failed converting filter to RegExp: ${e}`);
                }
            }
        }
        // TODO: why doesn't filters show up in app child logger?
        this.log('info', 'Now filtering.', filters);
        this.filters = filters;
    }


    /**
     * Starts the consume loop.
     */
    on_start() {
        // Already running
        if (this.running) {
            throw new Error('Cannot start, already started.');
        }
        else if (this.closing) {
            throw new Error('Cannot start, already closing.');
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
            this.kafkaConsumer.disconnect();
            delete this.kafkaConsumer;
        }

        // TODO: do we need this?  Probably if we want to disconnect
        // the socket on error.
        this.socket.disconnect(true);
    }


    /**
     * Consumes messages from Kafka until we find one that matches
     * the configured filters, and then returns that message.
     * If not filters are configured, this will just return
     * the first message from Kafka.
     */
    on_consume() {
        if (this.closing) {
            this.log('warn', 'Cannot consume, already closing.');
            return;
        }

        // Consume a message from Kafka
        return this.kafkaConsumer.consumeAsync()
        .then((kafkaMessage) => {
            // TODO build message async?
            // TODO: if this throws an error, we will end up returning
            // that error to the client's emit cb.  Should we always do this?
            const message = this.constructor._buildMessage(kafkaMessage);

            // If we got a message and it matches filters,
            // then return it now.
            if (message && this._match(message)) {
                return message;
            }
            // Else keep consuming in a loop until we find a match
            else {
                return this.on_consume();
            }
        })
        .catch((e) => {
            // If this looks like a node-rdkafka error
            if ('origin' in e && e.origin === 'kafka') {
                // Keep attempting to consume until we find a real message.
                switch (e.code) {
                    case kafka.CODES.ERRORS.ERR__PARTITION_EOF:
                    case kafka.CODES.ERRORS.ERR__TIMED_OUT:
                        this.log('debug', `Encountered innoculous Kafka error: ${e.message} (${e.code}), continuing`);
                        // Call on_consume again until we get an actual message;
                        return this.on_consume();
                }
            }
            // If we get here, we got a real error, bubble it up!
            throw e;
        });
    }


    /**
     * While this.running, calls on_consume() and then emits 'message' in a loop.
     */
    _loop() {
        if (!this.running) {
            this.log('debug', 'Consume loop stopping.');
            return Promise.resolve();
        }

        // Consume, emit the message, and then consume again.
        return this.on_consume()
        .then((message) => {
            this.socket.emit('message', message);
            this._loop();
        });
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
     * Logs error and returns a serialized error object suitable
     * for sending back to connected socket.io client.
     */
    _error(error, extra) {

        if (!(error instanceof Error)) {
            error = new Error(error);
        }
        this.log('error', error.message);

        error.socket = this.name;

        if (extra) {
            error = Object.assign(error, extra);
        }

        // should we remove the stack we serialize to send to the client?
        // delete err.stack;
        // TODO: Should we close the socket on errors?
        // this.close();
        return serializerr(error);
    }

    //  TODO: Should this wrap the message.payload json object?
    //  Should it place the topic, partition, offset field
    //  into message?
    static _buildMessage(kafkaMessage) {
        try {
            let message = objectutils.factory(kafkaMessage.message);
            // TODO: rename this?
            message._kafka = {
                'topic': kafkaMessage.topic,
                'partition': kafkaMessage.partition,
                'offset': kafkaMessage.offset,
                'key': kafkaMessage.key
            }
            return message;
        }
        catch (e) {
            throw new Error(`Failed building message from Kafka: '${kafkaMessage.message.toString()}' ${e}`);
        }
    }


}


module.exports = Kasocki;

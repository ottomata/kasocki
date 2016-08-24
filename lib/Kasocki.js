'use strict';

/**
 * Usage:
 * const Kasocki = require('kasocki');
 * io.on('connection', (socket) => {
 *     let kasocki = new Kasocki(socket, kafka_config, bunyan_logger);
 * });
 */


const bunyan      = require('bunyan');
const objectutils = require('./objectutils');
const kafka       = require('node-rdkafka');
const serializerr = require('serializerr');
const P           = require('bluebird');

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
 */
class Kasocki {
    /**
     * @param {socket.io Object} socket
     *
     * @param {Object} kafkaConfig suitable for passing to rdkafka.KafkaConsumer
     *                 constructor.  group.id and enable.auto.commit cannot
     *                 be provided and will be overridden.
     *
     * @param {Array}  allowedTopics.  Array of topic names that can be
     *                 subscribed to.  If this is not given, all topics are
     *                 allowed.
     *
     * @param {Object} logger.  bunyan Logger.  A child logger will be created
     *                 from this. If not provided, a new bunyan Logger will be
     *                 created.
     *
     * @constructor
     */
    constructor(
        socket,
        kafkaConfig,
        allowedTopics,
        logger
    ) {
        this.socket     = socket;
        this.name       = this.socket.id
        this.filters    = null;

        // True if consume _loop has started
        this.running    = false;
        // True if the kafkaConsumer has been subscribed or assigned
        this.subscribed = false;
        // True if close has been called
        this.closing    = false;


        this.allowedTopics = allowedTopics;

        var bunyanConfig = {
            'socket':      this.name,
            'serializers': bunyan.stdSerializers,
        };

        // If we are given a logger, assume it is a bunyan logger
        // and create a child.
        if (logger) {
            this.log = logger.child(bunyanConfig);
        }
        // Else create a new logger, with src logging enabled for dev mode
        else {
            this.log = bunyan.createLogger(
                Object.assign(bunyanConfig, {name: 'kasocki', src: true})
            );
        }

        this.log.info('Creating new Kasocki instance ' + this.name);

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

        // Create and connect a new KafkaConsumer instance
        this.constructor.createKafkaConsumer(kafkaConfig)

        // Save our consumer
        .then((consumer) => {
            this.kafkaConsumer = consumer;
        })

        // Register methods that start with on_ as socket event handlers.
        .then(this._registerHandlers.bind(this))

        // KafkaConsumer has connected, and handlers are registered,
        // tell the socket.io client we are ready.
        .then(() => {
            this.socket.emit('ready');
        })

        // If anything bad during initialization, disconnect now.
        .catch((e) => {
            this.log.error(e, 'Failed Kasocki initialization.');
            // TODO: communicate error back to client?
            this.on_disconnect();
        });
    }


    /**
     * Returns a Promise of a promisified and connected rdkafka.KafkaConsumer.
     * @param {Object} kafkaConfig
     * @param {Object} topicConfig
     * @return {Promise<KafkaConsumer>} Promisified KafkaConsumer
     */
    static createKafkaConsumer(kafkaConfig, topicConfig) {
        topicConfig = topicConfig || {}

        const consumer = P.promisifyAll(
            new kafka.KafkaConsumer(kafkaConfig, topicConfig)
        );
        return consumer.connectAsync(undefined)
        .then((metadata) => {
            return consumer;
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
            this.log.debug(`Registering socket event '${socketEvent}' to be handled by ${handler}`);

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
     * TODO: if we configure allowedTopics, maybe topic regexes shouldn't be allowed.
     * @param {Array} topics
     * @return {Array} topics as subscribed or assigned
     */
    on_subscribe(topics) {
        if (this.closing) {
            this.log.warn('Cannot subscribe, already closing.');
            return;
        }
        else if (this.subscribed) {
            const error = new Error(
                'Cannot subscribe, topics have already ' +
                'been subscribed or assigned.'
            );
            error.topics = topics;
            throw error;
        }

        if (!topics || topics.constructor.name != 'Array') {
            const error = new Error(
                'Must provide either an array topic names, or ' +
                ' an array of objects with topic, partition and offset.'
            );
            error.topics = topics;
            throw error;
        }

        // If we are given an array of objects, assume we are trying to assign
        // at a particular offset.  Note that this does not check that the topic
        // partition assignment makes any sense.  E.g. it is possible to
        // subscribe to non existent topic-partition this way.  In that case,
        // nothing will happen.
        // TODO: Better validation for topics
        if (topics[0].constructor.name == 'Object') {
            this._checkTopicsAllowed(topics.map(e => e.topic))
            this.log.info(
                {topics: topics},
                'Subscribing to topics, starting at assigned partition offsets.'
            );
            this.kafkaConsumer.assign(topics);
        }
        else {
            this._checkTopicsAllowed(topics);
            this.log.info(
                {topics: topics},
                'Subscribing to topics, starting at latest in each partition.'
            );
            this.kafkaConsumer.subscribe(topics);
        }

        this.subscribed = true;

        return topics;
    }

    /**
     * Throws an Error if this.allowedTopics is configured and
     * any of the topics are not in this list of allowed topics.
     *
     * @param {Array} topics
     * @throws Error if any of the topics are not allowed.
     */
    _checkTopicsAllowed(topics) {
        if (this.allowedTopics) {
            topics.map((topic) => {
                if (this.allowedTopics.indexOf(topic) < 0) {
                    throw new Error(`Topic '${topic}' is not available for consumption.`);
                }
            });
        }
    }

    /**
     * Iterates through filters.  If any filter looks like
     * a regex, then this will convert the string into a RegExp
     * and save it over the string.  this.filters will be
     * replaced by the list of filters given, and will be suitable
     * for passing to objectutils.match().
     *
     * @param {Array} filters
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
                this.log.debug(`Converting ${filter} to RegExp.`);
                try {
                    filters[key] = new RegExp(filter.substring(1, filter.length - 1));
                }
                catch(e) {
                    throw new Error(`Failed converting filter to RegExp: ${e}`);
                }
            }
        }

        this.log.info({filters: filters}, 'Now filtering.');
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
        else if (!this.subscribed) {
            throw new Error('Cannot start, topics have not been subscribed or assigned.');
        }
        else {
            this.running = true;

            // Loop until not this.running or until error.
            this.log.info('Starting');
            return this._loop();
        }
    }


    /**
     * Pauses the consume loop.  Does nothing
     * if not this.running.
     */
    on_pause() {
        this.log.info("Pausing.");
        this.running = false;
    }


    /**
     * Stops the consume loop, and closes the kafkaConsumer,
     * and disconnects the websocket.
     */
    on_disconnect() {
        this.log.info('Closing kafka consumer and disconnecting websocket.');
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
     * Consumes messages from Kafka until we find one that parses and matches
     * the configured filters, and then returns a Promise of that message.
     *
     *@return {Promise<Object>} Promise of first matched messaage
     */
    on_consume() {
        if (this.closing) {
            this.log.warn('Cannot consume, already closing.');
            return;
        }
        if (!this.subscribed) {
            throw new Error('Cannot consume. Topics have not been subscribed or assigned.');
        }

        // Consume a message from Kafka
        return this.kafkaConsumer.consumeAsync()

        // Parse value as JSON and augment it
        .then(this.constructor._buildMessage)

        // Match it against any configured filters
        .then(m => this.constructor._match(m, this.filters))

        // If we found a message, return it, else keep looking.
        .then((message) => {
            if (message) {
                return message;
            }
            else {
                this.log.trace('Consumed message did not match. Continuing');
                return this.on_consume();
            }
        })

        .catch((e) => {
            // If this looks like a node-rdkafka error
            if ('origin' in e && e.origin === 'kafka') {
                // Ignore innoculous Kafka errors.
                switch (e.code) {
                    case kafka.CODES.ERRORS.ERR__PARTITION_EOF:
                    case kafka.CODES.ERRORS.ERR__TIMED_OUT:
                        this.log.trace(e, `Encountered innoculous Kafka error: ${e.message} (${e.code}). Continuing.`);
                }
            }
            else {
                this.log.error(e, 'Caught error while attempting to consume a matched message. Continuing.');
            }
            // The kafkaMessage we consumed threw an error somewhere along the
            // way, but we don't care!  We want to return the first good message
            // we find to the consumer.  The error has been logged, let's
            // keep looking.
            return this.on_consume();
        });
    }


    /**
     * While this.running, calls on_consume() and then emits 'message' in a loop.
     */
    _loop() {
        if (!this.running) {
            this.log.debug('Consume loop stopping.');
            return Promise.resolve();
        }

        // Consume, emit the message, and then consume again.
        return this.on_consume()
        .then((message) => {
            this.socket.emit('message', message);
            this._loop();
        });
    }


    /**
     * Logs error and returns a serialized error object suitable
     * for sending back to connected socket.io client.
     */
    _error(error, extra) {
        if (!(error instanceof Error)) {
            error = new Error(error);
        }

        var errorLogObject = {err: error}

        if (extra) {
            // Include extra in the error that will be serialized for client.
            error = Object.assign(error, extra);
            // Add extra to the log object we will give to bunyan logger too.
            errorLogObject = Object.assign(errorLogObject, extra);
        }

        this.log.error(errorLogObject);

        // Add the socket name and delete the stack trace for the
        // error that will be sent to client.
        error.socket = this.name;
        delete error.stack;

        // TODO: Should we close the socket on errors?
        // this.close();
        return serializerr(error);
    }


    /**
     * Parses kafkaMessage.message as a JSON string and then
     * augments the object with extra data.  Returns this as a Promise.
     *
     * @param {KafkaMesssage} kafkaMessage
     * @return {Object}
     *
     *  TODO: Should this wrap the message.payload json object?
     *  Should it place the topic, partition, offset field
     *  into message?
     */
    static _buildMessage(kafkaMessage) {
        return new Promise((resolve, reject) => {
            let message = objectutils.factory(kafkaMessage.message);
            // TODO: rename this?
            message._kafka = {
                'topic': kafkaMessage.topic,
                'partition': kafkaMessage.partition,
                'offset': kafkaMessage.offset,
                'key': kafkaMessage.key
            }
            resolve(message);
        })
        .catch((e) => {
            // TODO:
            throw new Error(
                'Failed building message from Kafka in ' +
                `${kafkaMessage.topic}-${kafkaMessage.partition} ` +
                `at offset ${kafkaMessage.offset}. ` +
                `'${kafkaMessage.message.toString()}': ${e}`
            );
        });
    }


   /**
    * Matches obj against this.filters.  Returns a Promise of obj
    * if it matches, else a Promise that resolves to false.
    *
    * @param {Object} obj to match against
    * @return {Promise<Object>}
    */
   static _match(obj, filters) {
       return new Promise((resolve, reject) => {
           // Return the obj if filters aren't configured or if obj matches.
           if (obj && (!filters || objectutils.match(obj, filters))) {
               resolve(obj);
           }
           else {
               resolve(false);
           }
       });
   }
}


module.exports = Kasocki;

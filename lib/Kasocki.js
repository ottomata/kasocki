'use strict';

/**
 * Usage:
 * const Kasocki = require('kasocki');
 * io.on('connection', (socket) => {
 *     let kasocki = new Kasocki(socket, configOptions});
 * });
 */

const utils                  = require('./utils');
const objectutils            = require('./objectutils');


const errors                 = require('./error.js');
const KasockiError           = errors.KasockiError;
const InvalidTopicError      = errors.InvalidTopicError;
const TopicNotAvailableError = errors.TopicNotAvailableError;
const NotSubscribedError     = errors.NotSubscribedError;
const AlreadySubscribedError = errors.AlreadySubscribedError;
const AlreadyStartedError    = errors.AlreadyStartedError;
const AlreadyClosingError    = errors.AlreadyClosingError;
const InvalidFilterError     = errors.InvalidFilterError;
const InvalidMessageError    = errors.InvalidMessageError;

const kafka           = require('node-rdkafka');
const Promise         = require('bluebird');
const bunyan          = require('bunyan');
const serializerr     = require('serializerr');


/**
 * Represents a Kafka Consumer -> socket.io connection.
 *
 * This creates a new Kafka Consumer and passes consumed
 * messages to the connected socket.io client.
 *
 * Socket Handlers:
 * Any method that starts with 'on_' will be registered as a handler
 * for that socket event.  E.g. an on_subscribe function will be registered
 * as the handler for the 'subscribe' event.
 */
class Kasocki {
    /**
     * @param {socket.io Object} socket
     *
     * @param {Object}  options.
     *
     * @param {Object}  options.kafkaConfig suitable for passing to
     *                  rdkafka.KafkaConsumer constructor.  group.id and
     *                  enable.auto.commit be provided and will be overridden.
     *                  metadata.broker.list defaults to localhost:9092,
     *                  and client.id will also be given a sane default.
     *
     * @param {Object}  options.allowedTopics.  Array of topic names that can be
     *                  subscribed to.  If this is not given, all topics are
     *                  allowed.
     *
     * @param {Object}  options.logger.  bunyan Logger.  A child logger will be
     *                  created from this. If not provided, a new bunyan Logger
     *                  will be created.
     *
     * @param {Object}  options.kafkaEventHandlers: TODO
     *
     * @constructor
     */
    constructor(socket, options) {
        this.socket     = socket;
        this.name       = this.socket.id
        this.filters    = null;

        // True if the kafkaConsumer has been subscribed or assigned
        this.subscribed = false;

        // True if consume _loop has started
        this.running    = false;

        // True if close has been called
        this.closing    = false;

        const bunyanConfig = {
            'socket':      this.name,
            'serializers': bunyan.stdSerializers,
        };

        // If we are given a logger, assume it is a bunyan logger
        // and create a child.
        if (options.logger) {
            this.log = options.logger.child(bunyanConfig);
        }
        // Else create a new logger, with src logging enabled for dev mode
        else {
            this.log = bunyan.createLogger(
                Object.assign(bunyanConfig, {name: 'kasocki', src: true, level: 'debug'})
            );
        }

        this.log.info('Creating new Kasocki instance ' + this.name);

        // Default kafkaConfigs to use if not provided in kafkaConfig.
        // TODO: tune these.
        const defaultKafkaConfig = {
            'metadata.broker.list': 'localhost:9092',
            'client.id': `kasocki-${this.name}`
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
            'group.id': `kasocki-${this.name}`
        }

        // Merge provided over default configs.
        let kafkaConfig = Object.assign(defaultKafkaConfig, options.kafkaConfig);
        // Merge mandatory over provided configs.
        kafkaConfig = Object.assign(kafkaConfig, mandatoryKafkaConfig);

        // Create and connect a new KafkaConsumer instance
        utils.createKafkaConsumerAsync(kafkaConfig)

        // Save our consumer
        .then((consumer) => {
            if (options.kafkaEventHandlers) {
                // Object.keys(kafkaEventHandlers).foreach((event) => {
                //     // TODO Object.keys instead of for ... in preferred
                // })
                for (var event in options.kafkaEventHandlers) {
                    this.log.debug(
                        `Registering Kafka event ${event} to be handled by ` +
                        `function ${kafkaEventHandlers[event].name}`
                    );
                    consumer.on(event, options.kafkaEventHandlers[event]);
                }
            }
            this.kafkaConsumer = consumer;
        })

        // Save intersection of allowedTopics and existent topics.
        // These will be given to the client on ready so it
        // knows what topics are available.
        .then(() => {
            this.availableTopics = utils.getAvailableTopics(
                this.kafkaConsumer,
                options.allowedTopics
            );
            // Throw Error if there are no available topics.  This
            // will fail initialization and disconnect the client.
            if (this.availableTopics.length === 0) {
                let e = new KasockiError(
                    'No topics available for consumption. ' +
                    'This likely means that the configured allowedTopics ' +
                    'do not currently exist.'
                );
                e.allowedTopics = options.allowedTopics;
                throw e;
            }
        })

        // Register methods that start with on_ as socket event handlers.
        .then(this._registerHandlers.bind(this))

        // KafkaConsumer has connected, and handlers are registered,
        // tell the socket.io client we are ready and what topics
        // are available for consumption.
        .then(() => {
            this.socket.emit('ready', this.availableTopics);
        })

        // If anything bad during initialization, disconnect now.
        .catch((e) => {
            this.log.error(e, 'Failed Kasocki initialization.');
            // TODO: communicate error back to client?
            this.on_disconnect();
        });
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
     * TODO: consider making this '_wrapHandler' and have it return
     * a bound function.  Look into lodash for bind/currying. (_.partial)
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
        this.log.debug({socketEvent: socketEvent, arg: arg}, `Handling socket event ${socketEvent}`);
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
     * Assign topic partitions for consumption to this KafkaConsumer.
     * A Kasocki instance can only be subscribed once; its subscription
     * can not be changed.  Topic regexes are not supported.
     *
     * Note that this does not actually call KafkaConsumer.subscribe().  Instead
     * it uses KafkaConsumer.assign().  It does this in order to avoid getting
     * rebalances from Kafka.  A Kasocki instance always has a unique group.id,
     * so there will never be another consumer to balance with.
     * See: https://github.com/edenhill/librdkafka/issues/593
     *
     * Also see 'Notes on Kafka consumer state' in this repository's README.
     *
     * TODO: Better validation for topics
     *
     * @param {Array} topics
     * @return {Array} topics as subscribed or assigned
     */
    on_subscribe(topics) {
        if (this.closing) {
            this.log.warn('Cannot subscribe, already closing.');
            return;
        }
        else if (this.subscribed) {
            let error = new AlreadySubscribedError(
                'Cannot subscribe, topics have already ' +
                'been subscribed or assigned.'
            );
            error.topics = topics;
            throw error;
        }

        if (!topics ||
            (topics.constructor.name !== 'String' &&
            topics.constructor.name !== 'Array')
        ) {
            let error = new InvalidTopicError(
                'Must provide either a a string or array of topic names, or ' +
                ' an array of objects with topic, partition and offset.'
            );
            error.topics = topics;
            throw error;
        }
        // Convert to a single element Array if we were given a string.
        if (topics.constructor.name === 'String') {
            topics = [topics];
        }

        // If we are given an array of objects, assume we are trying to assign
        // at a particular offset.  Note that this does not check that the topic
        // partition assignment makes any sense.  E.g. it is possible to
        // subscribe to non existent topic-partition this way.  In that case,
        // nothing will happen.
        if (topics[0].constructor.name == 'Object') {
            // Check that topic names are allowed
            this._checkTopicsAllowed(topics.map(e => e.topic))
            this.log.info(
                {assignments: topics},
                'Subscribing to topics, starting at assigned partition offsets.'
            );
        }
        // Else topics is a simple Array of topic names.
        else {
            // Check that topic names are allowed.
            this._checkTopicsAllowed(topics);
            // Build topic-partition assignments starting at latest.
            topics = utils.buildAssignments(this.kafkaConsumer, topics);
            this.log.info(
                {assignments: topics},
                'Subscribing to topics, starting at latest in each partition.'
            );
        }

        // topics is now an array of assignemnts, so assign it.
        this.kafkaConsumer.assign(topics);
        this.subscribed = true;

        return topics;
    }

    //  TODO:
    // static _validateTopics(topics) {
//         if (!topics || topics.constructor.name != 'Array') {
//             let error = new InvalidTopicError(
//                 'Must provide either an array topic names, or ' +
//                 ' an array of objects with topic, partition and offset.'
//             );
//             error.topics = topics;
//             throw error;
//         }
//
//         topicType
//         topics.map((topic) => {
//             var topicName;
//             if (typeof topic === 'string') {
//                 topicName = topic;
//             }
//             else if (typeof topic === 'object') {
//                 topicName = topic.name;
//             }
//             else {
//                 throw new InvalidTopicError('Each topic must either be')
//             }
//         });
//     }

    /**
     * Throws an Error if any topic in topics Array is not
     * the list of available topics.
     *
     * @param {Array} topics
     * @throws Error if any of the topics are not available.
     */
    _checkTopicsAllowed(topics) {
        topics.forEach((topic) => {
            if (this.availableTopics.indexOf(topic) < 0) {
                throw new TopicNotAvailableError(
                    `Topic '${topic}' is not available for consumption.`
                );
            }
        });
    }


    /**
     * Iterates through filters.  If any filter looks like
     * a regex, then this will convert the string into a RegExp
     * and save it over the string.  this.filters will be
     * replaced by the list of filters given, and will be suitable
     * for passing to objectutils.match().
     * TODO: value in array filtering?
     * lookup json filter language?
     *
     * @param {Array} filters
     */
    on_filter(filters) {
        // Filters can be disabled.
        if (!filters) {
            this.filters = undefined;
            return;
        }

        if (!(filters instanceof Object)) {
            throw new InvalidFilterError(
                'Filters must be an Object of key, value pairs.'
            );
        }

        Object.keys(filters).forEach((key) => {
            const filter = filters[key]

            // Filters must be either strings or numbers.
            if (filter instanceof Object) {
                let error = new InvalidFilterError(
                    `Invalid filter for ${key}, cannot filter using an object.`
                );
                error.key = key;
                error.filter = filter;
                throw error;
            }

            // If this filter is a regex, it will begin and end with /
            if (filter.constructor.name === 'String' &&
                filter.charAt(0) === '/'   &&
                filter.charAt(filter.length - 1) === '/'
            ) {
                // Convert this to a regex and save it in filters.
                this.log.debug(`Converting ${filter} to RegExp for ${key}.`);
                try {
                    filters[key] = new RegExp(filter.substring(1, filter.length - 1));
                }
                catch(e) {
                    let error = new InvalidFilterError(`Failed converting filter to RegExp: ${e}`);
                    error.key = key;
                    error.filter = filter;
                    throw error;
                }
            }
        });

        this.log.info({filters: filters}, 'Now filtering.');
        this.filters = filters;
        // TODO: should this be returned to client?  Maybe if we
        // serialize RegExp nicely.
        // return this.filters;
    }


    /**
     * Starts the consume loop and returns a resolved Promise.
     * This starts a consume loop in the background, and as such
     * cannot report any error from it.
     * TODO: emit 'error' event if error during consume loop.
     *
     * @throws {AlreadyStartedError}
     * @throws {AlreadyClosingError}
     * @throws {NotSubscribedError}
     * @return {Promise} resolved.
     */
    on_start() {
        // Already running
        if (this.running) {
            throw new AlreadyStartedError('Cannot start, already started.');
        }
        else if (this.closing) {
            throw new AlreadyClosingError('Cannot start, already closing.');
        }
        else if (!this.subscribed) {
            throw new NotSubscribedError('Cannot start, topics have not been subscribed or assigned.');
        }
        else {
            this.running = true;

            // Loop until not this.running or until error.
            this.log.info('Starting');
            this._loop();
            // Return a resolved promise.
            // The consume loop will continue to run.
            return Promise.resolve();
        }
    }


    /**
     * Stops the consume loop.  Does nothing
     * if not this.running.
     */
    on_stop() {
        if (this.running) {
            this.log.info('Stopping.');
            this.running = false;
        }
        else {
            this.log.info(
                'Stop socket event received, but consume loop was not running'
            );
        }
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
     * TODO: can't use flowing mode with assign(), document this.
     *@return {Promise<Object>} Promise of first matched messaage
     */
    on_consume() {
        if (this.closing) {
            this.log.warn('Cannot consume, already closing.');
            return;
            // TODO not sure if we should throw here, this might be pretty normal.
            // throw new AlreadyClosingError('Cannot consume, already closing.');

        }
        if (!this.subscribed) {
            throw new NotSubscribedError('Cannot consume. Topics have not been subscribed or assigned.');
        }

        // Consume a message from Kafka
        return this.kafkaConsumer.consumeAsync()

        // Parse value as JSON and augment it
        // TODO: Allow buildMessage function to be configureable.
        .then(utils.buildMessageAsync)

        // Match it against any configured filters
        .then(m => objectutils.matchAsync(m, this.filters))

        // Catch Kafka errors, log and re-throw real errors,
        // ignore harmless ones.
        .catch({origin: 'kafka'}, (e) => {
            // Ignore innoculous Kafka errors.
            switch (e.code) {
                case kafka.CODES.ERRORS.ERR__PARTITION_EOF:
                case kafka.CODES.ERRORS.ERR__TIMED_OUT:
                    this.log.trace(e, `Encountered innoculous Kafka error: ${e.message} (${e.code}). Continuing.`);
                    break;
                default:
                    this.log.error(e, 'Caught Kafka error while attempting to consume a message.');
                    throw e;
            }
        })

        // Log and ignore InvalidMessageError.  We don't want to fail the client
        // if the data in a topic is bad.
        .catch(InvalidMessageError, (e) => {
            this.log.error(e);
        })

        // Any unexpected error will be thrown to the client will not be caught
        // here.  It will be returned to the client as error in the
        // socket.io event ack callback.

        // If we found a message, return it, else keep looking.
        .then((message) => {
            if (message) {
                this.log.trace({message: message}, 'Consumed message.');
                return message;
            }
            else {
                this.log.trace('Have not yet found a message while consuming, trying again.');
                return this.on_consume();
            }
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
            return this._loop();
        });
    }


    /**
     * Logs error and returns a serialized error object suitable
     * for sending back to connected socket.io client.
     *
     * @param  {Error}  error
     * @param  {Object} extra - extra information to be serialized as
     *                  fields on the error object.
     *
     * @return {Object} serialized error without stack.
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

        return serializerr(error);
    }

}


module.exports = Kasocki;

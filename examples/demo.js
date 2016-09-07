'use strict';

const Promise = require('bluebird');
const bunyan    = require('bunyan');

var log = bunyan.createLogger({
    name: 'KasockiDemoClient',
    level: 'info'
});


/**
 * Returns Promsified socket.io client connected to a Kasocki socket.io server.
 */
function createClient(port) {
    log.info(`Creating new Kasocki client, connecting to localhost:${port}`);
    return Promise.promisifyAll(
        require('socket.io-client')(`http://localhost:${port}/`)
    );
}


//  Read the demo to run from argv
var demoName = process.argv[2];
var topics = process.argv[3].split(',') || ['eqiad.mediawiki.revision-create'];


//  Create a new client, and run the demo when ready.
var client = createClient(6927);
client.on('ready', function(data){
    log.info(`Running ${demoName} demo...`);
    demos[demoName](client);
});

function emit(socketEvent, arg) {
    log.info(`Emitting socketEvent ${socketEvent}`);

    return client.emitAsync(socketEvent, arg)

    .then((res) => {
        log.info({socketEvent: socketEvent, result: res}, `${socketEvent} returned via socket.io ACK callback`);
    })

    .catch((err) => {
        log.error({socketEvent: socketEvent}, `${socketEvent} caught an error via socket.io ACK callback, disconnecting.`, err);
        // Disconect on error
        client.disconnect(true);
    });
}


const demos = {
    consume_one_message: (client) => {
        emit('subscribe', topics)
        .then(() => {
            return emit('consume', null);
        })
        .then(() => {
            client.disconnect();
        });
    },

    stream_for_three_seconds: (client) => {
        // Register an on message handler to log all received
        // after start is emitted.
        client.on('message', (msg) => {
            console.log(msg);
        });

        emit('subscribe', topics)
        .then(() => {
            return emit('start', null);
        })
        // consume the stream for 3 seconds then disconnect
        .delay(3000)
        .then(() => {
            client.disconnect();
        });
    },

    stream_with_filter: (client) => {
        // Register an on message handler to log all received
        // after start is emitted.
        client.on('message', (msg) => {
            console.log(msg);
        });

        emit('subscribe', topics)
        // Filter for revisions created by bots
        .then(() => {
            return emit('filter', {database: 'eswiki'});
            // return emit('filter', {'name': 'green'});
        })
        .then(() => {
            return emit('start', null);
        })
        // consume the stream for 3 seconds then disconnect
        .delay(3000)
        .then(() => {
            client.disconnect();
        });
    },

    stream_starting_from_offset: (client) => {
        // Register an on message handler to log all received
        // after start is emitted.
        client.on('message', (msg) => {
            console.log(msg);
        });

        let assignment = [
            {topic: 'eqiad.mediawiki.revision-create', partition: 0, offset: 6502717},
            // {topic: 'test', partition: 0, offset: 1928346},
        ]
        emit('subscribe', assignment)
        .then(() => {
            return emit('start', null);
        })
        // consume the stream for 3 seconds then disconnect
        .delay(3000)
        .then(() => {
            client.disconnect();
        });
    },
}


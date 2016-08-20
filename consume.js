'use strict';

/**
 * test consuming via socket.io
 */



var BBPromise = require('bluebird');

var port = process.argv[2];
var route = process.argv[3];
var uri = 'http://localhost:'+ port + route

var socket = require('socket.io-client')(uri)


function logErr(event, err, res) {
    if (err) {
        console.log(event, 'err: ', err);
        process.exit();
    }
    else {
        console.log(event, 'res: ', res);
    }
}

console.log('connecting to ' + uri);

socket.on('message', function(data){
    process.stdout.write(JSON.stringify(data) + "\n");
});

socket.on('error', function(data){
    process.stderr.write('ERROR', JSON.stringify(data) + "\n");
});


socket.on('disconnect', function(data){
    process.stdout.write("CLOSED.\n");
});


socket.on('ready', function(data){
    process.stdout.write("SO READY TO GO.\n");

    console.log("calling " + process.argv[4]);
    tests[process.argv[4]]();
});

var topics = ['test4'];

function emit(event, arg) {
    console.log(event, arg);
    if (event == 'disconnect') {
        socket.disconnect();
    }
    else {
        socket.emit(event, arg, logErr.bind(null, event));
    }
}


var tests = {
    simple: function() {
        emit('subscribe', topics);
        emit('start');
        BBPromise.delay(5000).then(emit.bind(null, 'pause'))
        .delay(5000).then(emit.bind(null, 'start'))
        .delay(5000).then(emit.bind(null, 'disconnect'))
    },

    runThrough: function() {
        emit('subscribe', topics);
        emit('start');

        BBPromise.delay(5000).then(emit.bind(null, 'pause'))
        .delay(2000).then(emit.bind(null, 'start'))
        .delay(3000).then(emit.bind(null, 'pause'))
        .delay(2000).then(emit.bind(null, 'subscribe', ['^test.*']))
        .delay(1000).then(emit.bind(null, 'start'))
        .delay(3000).then(emit.bind(null, 'filter', {'name': '/^(red|green)/'}))
        .delay(2000).then(emit.bind(null, 'disconnect'));
    },

    testSubscribeAfter: function() {
        emit('subscribe', ['test2'])
        emit('start');
        BBPromise.delay(3000).then(emit.bind(null, 'subscribe', ['^test.*']))
        .delay(3000).then(emit.bind(null, 'disconnect'));
    },

    closeThenSubscribe: function() {
        emit('subscribe', topics);
        emit('start');
        BBPromise.delay(3000).then(emit.bind(null, 'disconnect'))
        .then(emit.bind(null, 'subscribe', ['^test.*']));

    },

    withFilter: function() {
        emit('subscribe', ['^test.*'])
        emit('start');
        BBPromise.delay(3000).then(emit.bind(null, 'filter', {'name': 'A green door'} ))
        .delay(3000).then(emit.bind(null, 'subscribe', ['test2'] ))
        .delay(3000).then(emit.bind(null, 'filter', null))
        .delay(3000).then(emit.bind(null, 'disconnect'));
    },

    withRegexFilter: function() {
        emit('subscribe', ['^test.*'])
        emit('start');
        BBPromise.delay(3000).then(emit.bind(null, 'filter', {'name': '/green/'} ))
        .delay(3000).then(emit.bind(null, 'filter', null))
        .delay(3000).then(emit.bind(null, 'disconnect'));
    },

    withBadRegexFilter: function() {
        emit('subscribe', ['^test.*'])
        emit('start');
        BBPromise.delay(1000).then(emit.bind(null, 'filter', {'name': '/green(/'} ))
        .delay(1000).then(emit.bind(null, 'filter', null))
        .delay(3000).then(emit.bind(null, 'disconnect'));
    },

    withRegexAndLiteralFilter: function() {
        emit('subscribe', ['^test.*'])
        emit('start');
        BBPromise.delay(3000).then(emit.bind(null, 'filter', {'name': '/green/', 'why': 'because'} ))
        .delay(3000).then(emit.bind(null, 'filter', null))
        .delay(3000).then(emit.bind(null, 'disconnect'));
    },

    eventlogging: function() {
        emit('subscribe', ['eventlogging-valid-mixed'])
        emit('start');
        BBPromise.delay(10000).then(emit.bind(null, 'disconnect'));
    },

    justSubscribe: function() {
        emit('subscribe', topics);
        BBPromise.delay(2000).then(emit.bind(null, 'disconnect'));
    },

    justConsume: function() {
        emit('consume');
        BBPromise.delay(2000).then(emit.bind(null, 'disconnect'));
    },

    consumeIt: function() {
        emit('subscribe', topics);
        BBPromise.delay(1000).then(emit.bind(null, 'consume'))
        .delay(5000).then(emit.bind(null, 'disconnect'));
    },

    consumeFew: function() {
        emit('subscribe', topics);
        BBPromise.delay(1000).then(emit.bind(null, 'consume'))
        .then(emit.bind(null, 'consume'))
        .then(emit.bind(null, 'consume'))
        .delay(3000).then(emit.bind(null, 'disconnect'));
    },

    test: function() {
        socket.emit('test', 'amendme', (err, res) => {
            console.log('test got back:', err, res);
        });
    },

    justStart: function() {
        emit('start');
        BBPromise.delay(2000).then(emit.bind(null, 'disconnect'));
    },

    startFlow: function() {
        emit('start', ['test4']);
        BBPromise.delay(5000).then(emit.bind(null, 'disconnect'));
    },

    startFlowUnsubscribe: function() {
        emit('start', ['test4']);
        BBPromise.delay(5000).then(emit.bind(null, 'unsubscribe'))
        .delay(5000).then(emit.bind(null, 'disconnect'));
    },

    startFlowRestart: function() {
        emit('start', ['test4']);
        BBPromise.delay(5000).then(emit.bind(null, 'start', ['test2']))
        .delay(5000).then(emit.bind(null, 'disconnect'));
    },

    startSubscribeStart: function() {
        emit('start');
        BBPromise.delay(2000).then(emit.bind(null, 'subscribe', topics))
        .delay(2000).then(emit.bind(null, 'start'))
        .delay(2000).then(emit.bind(null, 'disconnect'));
    },

}
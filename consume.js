'use strict';

/**
 * test consuming via socket.io
 */



var BBPromise = require('bluebird');

var port = process.argv[2];
var route = process.argv[3];
var uri = 'http://127.0.0.1:'+ port + route

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


socket.on('disconnect', function(data){
    process.stdout.write("CLOSED.\n");
});


socket.on('ready', function(data){
    process.stdout.write("SO READY TO GO.\n");

    console.log("calling " + process.argv[4]);
    tests[process.argv[4]]();
});


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
        emit('subscribe', ['test']);
        emit('start');
        BBPromise.delay(5000).then(emit.bind(null, 'pause'))
        .delay(5000).then(emit.bind(null, 'start'))
        .delay(5000).then(emit.bind(null, 'disconnect'))
    },

    runThrough: function() {
        emit('subscribe', ['test']);
        emit('start');

        BBPromise.delay(5000).then(emit.bind(null, 'pause'))
        .delay(5000).then(emit.bind(null, 'start'))
        .delay(3000).then(emit.bind(null, 'filter', {'name': '/^(red|green)/'}))
        .delay(2000).then(emit.bind(null, 'disconnect'));
    },

    runThroughAssign: function() {
        emit('subscribe', [ { topic: 'test', partition: 0, offset: -1 } ]);
        emit('start');

        BBPromise.delay(5000).then(emit.bind(null, 'pause'))
        .delay(5000).then(emit.bind(null, 'start'))
        .delay(3000).then(emit.bind(null, 'filter', {'name': '/^(red|green)/'}))
        .delay(2000).then(emit.bind(null, 'disconnect'));
    },


    badSubscribe: function() {
        emit('subscribe', 'test2')
    },

    testSubscribeAfter: function() {
        emit('subscribe', ['test2'])
        emit('start');
        BBPromise.delay(3000).then(emit.bind(null, 'subscribe', ['^test.*']))
        .delay(3000).then(emit.bind(null, 'disconnect'));
    },

    closeThenSubscribe: function() {
        emit('subscribe', ['test']);
        emit('start');
        BBPromise.delay(3000).then(emit.bind(null, 'disconnect'))
        .then(emit.bind(null, 'subscribe', ['^test.*']));
    },

    withFilter: function() {
        emit('subscribe', ['test'])
        emit('start');
        BBPromise.delay(3000).then(emit.bind(null, 'filter', {'name': 'A green door'} ))
        .delay(3000).then(emit.bind(null, 'filter', null))
        .delay(3000).then(emit.bind(null, 'disconnect'));
    },

    withDottedFilter: function() {
        emit('subscribe', ['test'])
        emit('start');
        BBPromise.delay(3000).then(emit.bind(null, 'filter', {'user.name': 'mike'} ))
        .delay(3000).then(emit.bind(null, 'filter', null))
        .delay(3000).then(emit.bind(null, 'disconnect'));
    },

    withRegexFilter: function() {
        emit('subscribe', ['test'])
        emit('start');
        BBPromise.delay(3000).then(emit.bind(null, 'filter', {'name': '/green/'} ))
        .delay(3000).then(emit.bind(null, 'filter', null))
        .delay(3000).then(emit.bind(null, 'disconnect'));
    },

    withBadRegexFilter: function() {
        emit('subscribe', ['test'])
        emit('start');
        BBPromise.delay(1000).then(emit.bind(null, 'filter', {'name': '/green(/'} ))
        .delay(1000).then(emit.bind(null, 'filter', null))
        .delay(3000).then(emit.bind(null, 'disconnect'));
    },

    withRegexAndLiteralFilter: function() {
        emit('subscribe', ['test'])
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
        emit('subscribe', ['test']);
        BBPromise.delay(2000).then(emit.bind(null, 'disconnect'));
    },

    startTwice: function() {
        emit('subscribe', ['test']);
        BBPromise.delay(2000).then(emit.bind(null, 'start'))
        .delay(2000).then(emit.bind(null, 'start'))
        .then(emit.bind(null, 'disconnect'));
    },

    justConsume: function() {
        emit('consume');
        BBPromise.delay(2000).then(emit.bind(null, 'disconnect'));
    },

    consumeIt: function() {
        emit('subscribe', ['test']);
        BBPromise.delay(1000).then(emit.bind(null, 'consume'))
        .delay(5000).then(emit.bind(null, 'disconnect'));
    },

    consumeFew: function() {
        emit('subscribe', ['test']);
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

    startSubscribeStart: function() {
        emit('start');
        BBPromise.delay(2000).then(emit.bind(null, 'subscribe', ['test']))
        .delay(2000).then(emit.bind(null, 'start'))
        .delay(2000).then(emit.bind(null, 'disconnect'));
    },

    subscribeAllowed: function() {
        emit('subscribe', ['test', 'test6']);
        emit('start');
    },

    subscribeNotExist: function() {
        emit('subscribe', ['boogers']);
    },

    subscribeNotAllowed: function() {
        emit('subscribe', ['test3']);
    },

    assignAllowed: function() {
        emit('subscribe', [{'topic': 'test4', 'partition': 0, 'offset': 33326 }]);
    },

    assignNotAllowed: function() {
        emit('subscribe', [{'topic': 'nopers', 'partition': 0, 'offset': 33326 }]);
    },

    assign: function() {
        var assignments = [{'topic': 'test4', 'partition': 0, 'offset': 33326 }];

        emit('subscribe',assignments);
        emit('start');
    },

    assignEarliest: function() {
        emit('subscribe', [{'topic': 'test4', 'partition': 0, offset: 'earliest'}]);
        emit('start');
    },

    assignNoOffset: function() {
        emit('subscribe', [{'topic': 'test4', 'partition': 0}]);
        emit('start');
    },

    assignMulti: function() {
        emit('subscribe', [
            {'topic': 'test4', 'partition': 0, offset: 34695},
            {'topic': 'test', 'partition': 0, offset: 1146966},
            {'topic': 'test6', 'partition': 0, offset: 0},

        ]);
        emit('start');
    },

    assignMultiBadDataInTopic: function() {
        emit('subscribe', [
            {'topic': 'test4', 'partition': 0, offset: 33326},
            {'topic': 'test', 'partition': 0, offset: 1027270},
        ]);
        emit('start');
    },

}
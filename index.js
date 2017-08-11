'use strict';

var Promise = require("bluebird");

function newReader(context, opConfig, jobConfig) {
    var events = context.foundation.getEventEmitter();
    var jobLogger = context.logger;
    var consumer = context.foundation.getConnection({
        type: "kafka",
        endpoint: opConfig.connection,
        options: {
            type: "consumer",
            group: opConfig.group
        }
    }).client;

    return new Promise(function(resolve, reject) {
        consumer.on('ready', function() {
            jobLogger.info("Consumer ready");
            consumer.subscribe([opConfig.topic]);
            resolve(processSlice)
        });
    });

    function processSlice(partition, logger) {
        return new Promise(function(resolve, reject) {
            var consuming = setInterval(consume, opConfig.interval);
            var slice = [];
            var iteration_start = Date.now();

            consumer.on('data', receiveData);
            consumer.on('error', error);

            events.on('worker:shutdown', finishSlice);

            // This is going to have an issue that data will still be coming in
            // while the chunk is being processed.

            // Kick of initial processing.
            consume();


            function clearListeners() {
                clearInterval(consuming);
                consumer.removeListener('data', receiveData);
                consumer.removeListener('error', error);
                events.removeListener('worker:shutdown', finishSlice);
            }

            function finishSlice() {
                clearListeners();
                consumer.disconnect();
                resolve(slice);
            }

            function completeSlice() {
                clearListeners();
                logger.warn(`Resolving with ${slice.length} results`);
                resolve(slice);
            }

            function receiveData(data) {
                slice.push(data.value);

                if (slice.length >= opConfig.size) {
                    completeSlice();
                }
            }

            function error(err) {
                logger.error(err);
                clearListeners();
                reject(err);
            }

            function consume() {
                if (((Date.now() - iteration_start) > opConfig.wait) || (slice.length >= opConfig.size)) {
                    completeSlice();
                }
                else {
                    consumer.consume(opConfig.size - slice.length);
                }
            }

        });
    }
}

function newSlicer(context, job, retryData, slicerAnalytics, logger) {
    // The slicer actually has no work to do here.
    return Promise.resolve([function() {
        return new Promise(function(resolve, reject) {
            // We're using a timeout here to slow down the rate that slices
            // are created otherwise it swamps the queue on startup. The
            // value returned is meaningless but we still need something.
            setTimeout(function() {
                resolve(1);
            }, 100)
        });
    }]);
}

function schema() {
    return {
        topic: {
            doc: 'Name of the Kafka topic to process',
            default: '',
            format: 'required_String'
        },
        group: {
            doc: 'Name of the Kafka consumer group',
            default: '',
            format: 'required_String'
        },
        size: {
            doc: 'How many records to read before a slice is considered complete.',
            default: 10000,
            format: Number
        },
        wait: {
            doc: 'How long to wait for a full chunk of data to be available. Specified in milliseconds.',
            default: 30000,
            format: Number
        },
        interval: {
            doc: 'How often to attempt to consume `size` number of records. This only comes into play if the initial consume could not get a full slice.',
            default: 1000,
            format: Number
        },
        connection: {
            doc: 'The Kafka consumer connection to use.',
            default: '',
            format: 'required_String'
        }
    };
}

module.exports = {
    newReader: newReader,
    newSlicer: newSlicer,
    schema: schema,
    parallelSlicers: false
};

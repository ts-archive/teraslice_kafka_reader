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
        var shuttingdown = false;

        consumer.on('ready', function() {
            jobLogger.info("Consumer ready");
            consumer.subscribe([opConfig.topic]);

            resolve(processSlice)
        });

        function processSlice(partition, logger) {
            return new Promise(function(resolve, reject) {
                var slice = [];
                var iteration_start = Date.now();
                var consuming = setInterval(consume, opConfig.interval);

                // Listeners are registered on each slice and cleared at the end.
                function clearListeners() {
                    clearInterval(consuming);
                    consumer.removeListener('data', receiveData);
                    consumer.removeListener('error', error);
                    events.removeListener('worker:shutdown', shutdown);
                }

                // Called when the job is shutting down but this occurs before
                // slice:success is called so we need to tell the handler we're
                // shuttingdown.
                function shutdown() {
                    completeSlice();
                    shuttingdown = true;
                }

                // Called when slice processing is completed.
                function completeSlice() {
                    clearListeners();
                    jobLogger.warn(`Resolving with ${slice.length} results`);
                    resolve(slice);
                }

                function error(err) {
                    jobLogger.error(err);
                    clearListeners();
                    reject(err);
                }

                // Process a chunk of data received from Kafka
                function receiveData(data) {
                    slice.push(data.value);

                    if (slice.length >= opConfig.size) {
                        completeSlice();
                    }
                }

                function consume() {
                    if (((Date.now() - iteration_start) > opConfig.wait) || (slice.length >= opConfig.size)) {
                        completeSlice();
                    }
                    else {
                        consumer.consume(opConfig.size - slice.length);
                    }
                }

                // We only want to move offsets if the slice is successful.
                function commit() {
                    consumer.commit();

                    if (shuttingdown) {
                        consumer.disconnect();
                    }

                    // This can't be called in clearListners as it must exist
                    // after processing of the slice is complete.
                    events.removeListener('slice:success', commit);
                }

                consumer.on('data', receiveData);
                consumer.on('error', error);

                events.on('worker:shutdown', shutdown);
                events.on('slice:success', commit);

                // Kick off initial processing.
                consume();
            });
        }
    });
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

'use strict';

const Promise = require('bluebird');

function newReader(context, opConfig) {
    const events = context.foundation.getEventEmitter();
    const jobLogger = context.logger;
    const consumer = context.foundation.getConnection({
        type: 'kafka',
        endpoint: opConfig.connection,
        options: {
            type: 'consumer',
            group: opConfig.group
        },
        topic_options: {
            'enable.auto.commit': false
        }
    }).client;

    return new Promise(((resolve) => {
        let shuttingdown = false;

        consumer.on('ready', () => {
            jobLogger.info('Consumer ready');
            consumer.subscribe([opConfig.topic]);

            // for debug logs.
            consumer.on('event.log', (event) => {
                jobLogger.info(event);
            });

            resolve(processSlice);
        });

        function processSlice() {
            return new Promise(((resolveSlice, reject) => {
                const slice = [];
                const iterationStart = Date.now();
                const consuming = setInterval(consume, opConfig.interval);

                let blocking = false;

                // Listeners are registered on each slice and cleared at the end.
                function clearListeners() {
                    clearInterval(consuming);
                    // consumer.removeListener('data', receiveData);
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

                    resolveSlice(slice);
                }

                function error(err) {
                    jobLogger.error(err);
                    clearListeners();
                    reject(err);
                }

                function consume() {
                    // If we're blocking we don't want to complete or read
                    // data until unblocked.
                    if (blocking) return;

                    if (((Date.now() - iterationStart) > opConfig.wait) ||
                        (slice.length >= opConfig.size)) {
                        completeSlice();
                    } else if (!blocking) {
                        // We only want one consume call active at any given time
                        blocking = true;

                        // Our goal is to get up to opConfig.size messages but
                        // we may get less on each call.
                        consumer.consume(opConfig.size - slice.length, (err, messages) => {
                            if (err) {
                                // logger.error(err);
                                reject(err);
                                return;
                            }

                            messages.forEach((message) => {
                                slice.push(message.value);
                            });

                            if (slice.length >= opConfig.size) {
                                completeSlice();
                            } else {
                                blocking = false;
                            }
                        });
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

                consumer.on('error', error);

                events.on('worker:shutdown', shutdown);
                events.on('slice:success', commit);

                // Kick off initial processing.
                consume();
            }));
        }
    }));
}

function slicerQueueLength() {
    // Queue is not really needed so we just want the smallest queue size available.
    return 'QUEUE_MINIMUM_SIZE';
}

function newSlicer() {
    // The slicer actually has no work to do here.
    return Promise.resolve([() => new Promise((resolve) => {
        // We're using a timeout here to slow down the rate that slices
        // are created otherwise it swamps the queue on startup. The
        // value returned is meaningless but we still need something.
        setTimeout(() => {
            resolve(1);
        }, 100);
    })]);
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
    newReader,
    newSlicer,
    schema,
    slicerQueueLength
};

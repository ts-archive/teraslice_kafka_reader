'use strict';

var Promise = require("bluebird");

function newReader(context, opConfig, jobConfig) {
    var consumer_ready = false;
    var subscribed = false;
    var consumer;

    return function(partition, logger) {
        if (! consumer_ready) {
            consumer = context.foundation.getConnection({
                type: "kafka",
                endpoint: opConfig.connection
            }).client;

            consumer.on('ready', function() {
                consumer_ready = true;

                logger.info("Consumer ready");
            });
        }

        // We have to wait for the consumer to be ready. After the
        // first slice this usually isn't an issue.
        var subscriber = setInterval(function() {
            if (consumer_ready && ! subscribed) {
                logger.info("Subscribing to topic " + opConfig.topic);
                clearInterval(subscriber);
                consumer.subscribe([opConfig.topic]);
                subscribed = true;
            }
        }, 10);

        return new Promise(function(resolve, reject) {
            var slice = [];
            var iteration_start = Date.now();

            function completeSlice() {

                clearInterval(consuming);

                //consumer.unsubscribe([opConfig.topic]);
                consumer.removeListener('data', receiveData);
                consumer.removeListener('error', error);

                logger.info("Resolving with " + slice.length + " results.");

                resolve(slice);
            }

            function receiveData(data) {
                slice.push(data.value);

                //console.log(data.value.toString());

                if (slice.length >= opConfig.size) {
                    completeSlice();
                }
            }

            function error(err) {
                logger.error(err);
                reject(err);
            }

            var consuming = setInterval(function() {
                if (subscribed) {
                    if (((Date.now() - iteration_start) > opConfig.wait) || (slice.length >= opConfig.size)) {
                        completeSlice();
                    }
                    else {
                        consumer.consume(opConfig.size - slice.length);
                    }
                }
            }, 1000)

            // This is going to have an issue that data will still be coming in
            // while the chunk is being processed.
            consumer.on('data', receiveData);

            consumer.on('error', error);
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

function schema(){
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
            default: 10,
            format: Number
        },
        wait: {
            doc: 'How long to wait for a full chunk of data to be available. Specified in milliseconds.',
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

const fs = require("fs");
const config = require('config');
const { parse } = require("csv-parse");
const { Kafka, logLevel, KafkaMessage } = require('kafkajs')

// read config parameter
const csvFile = config.get('Application.CsvFile');
const kafkaClientId = config.get('Application.Kafka.ClientId');
const kafkaCluster = config.get('Application.Kafka.Cluster');
const kafkaTopic = config.get('Application.Kafka.Topic');

const kafka = new Kafka({
    logLevel: logLevel.INFO,
    clientId: kafkaClientId,
    /*brokers: ['PLOGKAF01A:9092', 'PLOGKAF02A:9092', 'PLOGKAF03A:9092'],*/
    /*brokers: ['CLOGKAF01A:9092', 'CLOGKAF02A:9092', 'CLOGKAF03A:9092'],*/
    brokers: kafkaCluster.split(','),
    connectionTimeout: 60000,
    requestTimeout: 60000,
    retry: {
        initialRetryTime: 30000,
        maxRetryTime: 1800000,
        retries: 60,
    },
    authenticationTimeout: 30000,
});

const producer = kafka.producer();
let counter = 0;
let messagesent = 0;
let canshutdown = false;
let exitimmediate = false;

const sendMessage = async (row) => {

    let payload = row[0];
    let hdr = {};

    for (let i = 1; i < row.length;) {
        hdr[row[i++].trim()] = row[i++];
    }

    await producer.send({
        topic: 'SDH.Request',
        acks: 1,
        timeout: 30000,
        retry: 60,
        messages: [{
            partition: 0,
            value: payload,
            headers: hdr
        }]
    }).then(response => {
        undefined
        /*kafka.logger().info(`Messages sent`, {
            response            
        })*/
    })
        .catch(e => kafka.logger().error(`[csv2kafka/producer] ${e.message}`, { stack: e.stack }));
    messagesent++;
}

const shutdown = async () => {
    await producer.disconnect();
    process.exit(0);
}

const run = async () => {

    kafka.logger().info("Started...");
    kafka.logger().info('kafka cluster               = ' + kafkaCluster);
    kafka.logger().info('kafka topic                 = ' + kafkaTopic);
    kafka.logger().info('csv file                    = ' + csvFile);

    await producer.connect();
    fs.createReadStream(csvFile)
        .pipe(parse({ delimiter: ";", from_line: 2 }))
        .on("data", function (row) {
            counter++;
            sendMessage(row);
            /*console.log("message #" + counter + " has been sent.");*/
        })
        .on("end", function () {
            kafka.logger().info("Finished. Flushing #" + counter + " messages. Please Wait.");
            canshutdown = true;
        })
        .on("error", function (error) {
            console.log(error.message);
            canshutdown = true;
            exitimmediate = true;
        });

    setInterval(() => {
        kafka.logger().info('can shutdown = ' + canshutdown + ' counter = ' + counter + ' msg sent = ' + messagesent);
        if (canshutdown == true && (counter == messagesent || exitimmediate == true))
            shutdown();
    }, 1000);

}

run().catch(e => kafka.logger().error(`[csv2kafka/producer] ${e.message}`, { stack: e.stack }))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
    process.on(type, async e => {
        try {
            kafka.logger().info(`process.on ${type}`);
            kafka.logger().error(e.message, { stack: e.stack });
            shutdown();
        } catch (_) {
            process.exit(1);
        }
    })
})

signalTraps.map(type => {
    process.once(type, async () => {
        console.log('');
        kafka.logger().info('[csv2kafka/producer] disconnecting');
        shutdown();
    })
})
const fs = require("fs");
const config = require('config');
const { parse } = require("csv-parse");
const { Kafka, logLevel, KafkaMessage } = require('kafkajs')
const { pipeline } = require('stream');


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

    try {

        await producer.send({
            topic: kafkaTopic,
            acks: 1,
            timeout: 30000,
            retry: 60,
            messages: [{
                partition: 0,
                //key: JSON.parse(payload).codiceStatoOperazionale,
                value: payload,
                headers: hdr
            }]
        });
            
        messagesent++;
    }
    catch(e) {
        kafka.logger().error(`[csv2kafka/producer] ${e.message}`, { stack: e.stack });    
    }
}

const shutdown = async () => {
    await producer.disconnect();
    process.exit(0);
}

const send = (csvreadstream,csvparseablestream) => {
    return new Promise( (resolve,reject) => {
        pipeline (
            csvreadstream,csvparseablestream,(err) => {
                if (err) {
                    kafka.logger().error('error(s) occurred in pipeline');
                    reject;
                } else {                    
                    resolve(err);
                }
            });
        }
    );        
}

const run = async () => {

    kafka.logger().info("Started...");
    kafka.logger().info('kafka cluster               = ' + kafkaCluster);
    kafka.logger().info('kafka topic                 = ' + kafkaTopic);
    kafka.logger().info('csv file                    = ' + csvFile);

    await producer.connect();
    
    const csvreadstream = fs.createReadStream(csvFile);
    const csvparseablestream = parse({ delimiter: ";", from_line: 2 });
    
    csvparseablestream
    .on("end", function () {
        kafka.logger().info("Finished. Flushing #" + (counter-messagesent) + " messages. Please Wait.");
        canshutdown = true;
    })
    .on("error", function (error) {
        console.log(error.message);
        canshutdown = true;
        exitimmediate = true;
    })
    .on("data", function (row) {
        counter++;
        sendMessage(row);
        if (counter % 10000 === 0) {
            kafka.logger().info('counter = '+counter+' msg sent = '+messagesent);
        }
    });                    

    await send(csvreadstream,csvparseablestream);
    kafka.logger().info('counter = '+counter+' msg sent = '+messagesent);
    //csvreadstream.pipe(csvparseablestream);

    setInterval(() => {
        kafka.logger().info('can shutdown = ' + canshutdown + ' counter = ' + counter + ' msg sent = ' + messagesent);
        if ((canshutdown == true && counter == messagesent) || exitimmediate == true)
            shutdown();
    }, 10000);

}

run().catch(e => kafka.logger().error(`[csv2kafka/producer] ${e.message}`, { stack: e.stack }));

const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

errorTypes.map(type => {
    process.on(type, async e => {
        try {
            kafka.logger().info(`process.on ${type}`);
            kafka.logger().error(e.message, { stack: e.stack });
            shutdown();
        } catch (_) {
            process.exit(1);
        }
    });
});

signalTraps.map(type => {
    process.once(type, async () => {
        console.log('');
        kafka.logger().info('[csv2kafka/producer] disconnecting');
        shutdown();
    });
});
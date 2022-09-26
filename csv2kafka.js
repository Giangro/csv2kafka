const fs = require("fs");
const { parse } = require("csv-parse");
const { Kafka, logLevel } = require('kafkajs')

const stato = 'SEPMN023B';
const contentType = 'application/json';
const spring_json_header_types = '{"stato":"java.lang.String","codiceOdl":"java.lang.String","partitionKey":"java.lang.String","confCliente":"java.lang.Integer","codiceOggettoPostale":"java.lang.String","contentType":"java.lang.String"}';

const kafka = new Kafka({
    logLevel: logLevel.INFO,    
    clientId: 'toolIO',
    /*brokers: ['PLOGKAF01A:9092', 'PLOGKAF02A:9092', 'PLOGKAF03A:9092'],*/
    /*brokers: ['CLOGKAF01A:9092', 'CLOGKAF02A:9092', 'CLOGKAF03A:9092'],*/
    brokers: ['localhost:9092'],
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

const sendMessage = async (row) => {

    let codiceOggettoPostale = row[0];
    let codiceOdl = row[1];
    let partitionKey = codiceOdl;
    let confCliente = row[2];

    return producer.send({
        topic: 'SEP.OdlMessoOutbound',
        acks: 1,
        timeout: 30000,
        retry: 60,
        messages: [{
            partition: 0,
            /*key: partitionKey,*/
            value: '{"codiceOggettoPostale":"' + codiceOggettoPostale + '"}',
            headers: {
                'stato': '"' + stato + '"',
                'codiceOdl': '"' + codiceOdl + '"',
                'partitionKey': '"' + partitionKey + '"',
                'confCliente': confCliente,
                'codiceOggettoPostale': '"' + codiceOggettoPostale + '"',
                'contentType': '"' + contentType + '"',
                'spring_json_header_types': spring_json_header_types
            }
        }]
    })    
}

const shutdown = async () => {
    await producer.disconnect();
    process.exit(0);
}

const run = async () => {
    console.log("Started...");
    await producer.connect();    
    fs.createReadStream("./Battente.csv")
        .pipe(parse({ delimiter: ";", from_line: 2 }))
        .on("data", function (row) {
            counter++;
            sendMessage(row)
                .then(response => {
                    undefined
                    /*kafka.logger().info(`Messages sent`, {
                    response            
                    })*/
                })
                .catch(e => kafka.logger().error(`[csv2kafka/producer] ${e.message}`, { stack: e.stack }));
            /*console.log("message #" + counter + " has been sent.");*/
        })
        .on("end", function () {            
            console.log("Finished. Flushing #" + counter + " messages. Please Wait.");
            shutdown();
        })
        .on("error", function (error) {            
            console.log(error.message);
            shutdown();
        });

}

run().catch(e => kafka.logger().error(`[csv2kafka/producer] ${e.message}`, { stack: e.stack }))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      kafka.logger().info(`process.on ${type}`)
      kafka.logger().error(e.message, { stack: e.stack })
      await producer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    console.log('')
    kafka.logger().info('[csv2kafka/producer] disconnecting')
    clearInterval(intervalId)
    await producer.disconnect()
  })
})
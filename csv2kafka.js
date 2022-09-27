const fs = require("fs");
const { parse } = require("csv-parse");
const { Kafka, logLevel, KafkaMessage } = require('kafkajs')

//const stato = 'SEPMN023B';
//const contentType = 'application/json';
//const spring_json_header_types = '{"stato":"java.lang.String","codiceOdl":"java.lang.String","partitionKey":"java.lang.String","confCliente":"java.lang.Integer","codiceOggettoPostale":"java.lang.String","contentType":"java.lang.String"}';

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
let messagesent = 0;
let canshutdown = false;
let exitimmediate = false;

const sendMessage = async (row) => {

    let payload = row[0];
    let hdr = {};

    for (let i=1;i<14;) {        
        hdr[row[i++]] = row[i++];
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
    console.log("Started...");
    await producer.connect();    
    fs.createReadStream("./sdhrequest.csv")
        .pipe(parse({ delimiter: ";", from_line: 2 }))
        .on("data", function (row) {
            counter++;
            sendMessage(row);                
            /*console.log("message #" + counter + " has been sent.");*/
        })
        .on("end", function () {            
            console.log("Finished. Flushing #" + counter + " messages. Please Wait.");
            canshutdown=true;        
        })
        .on("error", function (error) {            
            console.log(error.message);
            canshutdown=true;
            exitimmediate=true;
        });

        setInterval(()=>{
            console.log('can shutdown = '+canshutdown+' counter = '+counter+' msg sent = '+messagesent);  
            if (canshutdown==true && (counter == messagesent || exitimmediate == true))
                shutdown();          
        }, 1000);
        
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
    await producer.disconnect()
  })
})
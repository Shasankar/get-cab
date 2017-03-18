var kafka = require('kafka-node');
var Producer = kafka.Producer;
var client = new kafka.Client();
var producer = new Producer(client);

producer.on('ready',function (){
    console.log('Ready to push cptn locations to kafka!');
});

var Consumer = kafka.Consumer;
var payloads = [{topic: 'custCptnLoc'}];        /*parameterize the topic*/
var consumer = new Consumer(client,payloads);
var consumerMessages = [];
consumer.on('message',function(message){
    console.log(message);
    consumerMessages.push(message.value);
    console.log(consumerMessages);
});

module.exports = {producer: producer,
    consumerMessages: consumerMessages};
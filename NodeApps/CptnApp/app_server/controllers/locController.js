var queue = require('../models/kafka').producer;
var custCptn = require('../models/kafka').consumerMessages;

module.exports.queloc = function(ws, req){
    ws.on('message',function(msg){
        console.log(msg);
        var payloads = [{topic: 'cptnLoc',         /*parameterize the topic*/
            messages: msg}];
        queue.send(payloads,function(err,data){
            var matchCombo = {};
            custCptn.forEach(function(item){
                console.log(item.split(',')[1].slice(1));
                console.log(msg.split(',')[0]);                
                if(item.split(',')[1].slice(1) === msg.split(',')[0]){ 
                    console.log('match found');
                    matchCombo.cust=item.replace(/\(|\)/g,'').split(',')[0];                  
                    matchCombo.dist=item.replace(/\(|\)/g,'').split(',')[2];
                    console.log(matchCombo);
                    ws.send(JSON.stringify(matchCombo));
                }
            });
        });
    });
}
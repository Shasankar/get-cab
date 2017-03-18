var queue = require('../models/kafka').producer;
var async = require('async');
var custCptn = require('../models/kafka').consumerMessages;

module.exports.getCar = function(req, res, next) {
    console.log(req.body.id);
    console.log(req.body.lat);
    console.log(req.body.long);
    
    var locMessage = req.body.id+','+req.body.lat+','+req.body.long;
    var payloads = [{topic: 'custLoc',         /*parameterize the topic*/
            messages: locMessage}];
    queue.send(payloads,function(err,data){
        console.log(data);
        console.log('waiting for match');
        
        var match = false;
        var matchCombo = {};
        async.until(function(){
            return match;
        },function(callback){
            console.log(custCptn);
            custCptn.forEach(function(item){
                console.log(item.split(',')[0].slice(1));
                console.log(req.body.id);                
                if(item.split(',')[0].slice(1) === req.body.id){ 
                    console.log('match found');
                    match=true; 
                    matchCombo.cab=item.replace(/\(|\)/g,'').split(',')[1];                  
                    matchCombo.dist=item.replace(/\(|\)/g,'').split(',')[2];
                    console.log(matchCombo);
                }
            });
            setTimeout(callback,3000);       /*parameterize the interval*/
        },function(){ 
            res.render('cabDetails', { title: 'Get Cab', matchCombo: matchCombo });                   
        });
    });
}
angular.module('cptnApp',[]);
var cptnController = function ($scope,$interval) {
    $scope.cptnId = Math.floor(Math.random() * 5 + 100);
    $scope.location = {
        lat: Math.floor(Math.random() * 5),
        long: Math.floor(Math.random() * 5)
    }
    $scope.matchCombo = {};
    
    var ws=new WebSocket('ws://localhost:3001/liveloc');
    ws.onmessage = function(msg){
        console.log(msg.data);
        $scope.matchCombo = JSON.parse(msg.data);
    };
    var dir='right';
    $interval(function(){
        //set direction
        if(dir === 'right' && $scope.location.lat < 5)
            dir = 'right';
        else if(dir === 'right' && $scope.location.lat >= 5)
            dir = 'left';
        else if(dir === 'left' && $scope.location.lat > 0)
            dir = 'left';
        else if(dir === 'left' && $scope.location.lat <= 0)
            dir = 'right';
        
        //go direction
        if(dir === 'right')
            $scope.location.lat ++;
        if(dir === 'left')
            $scope.location.lat --;   

        ws.send($scope.cptnId + ',' + $scope.location.lat + ',' + $scope.location.long)     
    },10000)
};
angular.module('cptnApp').controller('cptnCtrl',cptnController);
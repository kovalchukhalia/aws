
var SQS = function(sqs, QueueUrl){
  this.sqs = sqs;
  this.QueueUrl = QueueUrl;
}

SQS.prototype.sendMessage = function(message, callback) {
  this.sqs.sendMessage(
        {
          MessageBody: message,
          "QueueUrl": this.QueueUrl    
        },
        onMessageSendConfirmed(callback)
      );
}


var onMessageSendConfirmed = function(callback) {
  return function(err, data){
    if(err) {console.log(err); callback(err); return;}     
     callback(null, data);
  }
}


SQS.prototype.receiveMessages = function(maxMessages, callback){
  	this.sqs.receiveMessage(
      {
        "QueueUrl": this.QueueUrl,
        "MaxNumberOfMessages": maxMessages,
        "VisibilityTimeout": 30,
        "WaitTimeSeconds": 20
      },
      onMessageRecievied(this,callback)
    );
}

SQS.prototype.receiveMessage = function(callback){  
    this.receiveMessages(1, 
      function(err, messages) {
        if(err) {
          callback(err);
          return;
        }else {
          var singleMessage = messages[0];
          callback(null, singleMessage);
        }
      });
}


var onMessageRecievied = function(sqsObj, callback) {
  return function(err, data){
    if(err) { callback(err); return;}
      if(data.Messages){
        for(var i = 0; i < data.Messages.length; i++){        
          sqsObj.sqs.deleteMessage({
                  "QueueUrl" : sqsObj.QueueUrl,
                  "ReceiptHandle" :data.Messages[i].ReceiptHandle
                }, function(err){
                  if(err) { callback(err); return;}      
                  callback(null, data.Messages);                    
                });   
        }
      }else {callback("no messages in queue"); }          
  }
}

module.exports = SQS;
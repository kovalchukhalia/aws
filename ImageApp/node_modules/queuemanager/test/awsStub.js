var sinon = require("sinon");

module.exports = function(){
	var AWS = {};
	AWS.SQS = function(){};
	AWS.S3 = function(){};

var SQSObject = {}
	SQSObject.receiveMessage = function(){};
	SQSObject.sendMessage = function(){};
	SQSObject.deleteMessage = function(){};

	sinon.stub(AWS, "SQS").returns(SQSObject);
	
	
	return AWS;

}
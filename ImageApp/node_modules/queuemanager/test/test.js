var assert = require("assert");
var sinon = require("sinon");
var awsstub = require("./awsStub")();

var Queue = require	("../queuemanager");


describe("queuemanager",function(){		
		var sqsstub;
		var queueURL;
		beforeEach(function(){
				queueURL = "https:\\samplequeue.com";
				sqsstub = new awsstub.SQS();
				queue = new Queue(sqsstub, queueURL);				
		});

		describe("sendMessage",function(){
			it("should call a sqs.sendMessage once ", function(){							
				sqsstub.sendMessage = sinon.mock().once();

				queue.sendMessage("messagebody", function() {});
			})
		});

		describe("sendMessage",function(){
			it("should call a callback on success send", function(){			
				var sendMessageData = {messageId: "sampleMessageId", MD5OfMessageBody:"sampleMD5"}
				sqsstub.sendMessage = sinon.mock().callsArgWith(1, null, sendMessageData);

				queue.sendMessage("messagebody", function(err, data) {
					assert.equal(err, null, err + " : " + null);
					assert.equal(data, sendMessageData, data + " : " + sendMessageData);
				});
			})
		});

		describe("sendMessage",function(){
			it("should call a callback on error send", function(){			
				var sendError = "error"
				sqsstub.sendMessage = sinon.mock().callsArgWith(1, sendError);

				queue.sendMessage("messagebody", function(err, data) {
					assert.equal(err, sendError, err + " : " + sendError);
					assert.equal(data, undefined, data + " : " + undefined);
				});
			})
		});

		describe("receiveMessage",function(){
			it("should call a sqs.receiveMessages once ", function(){							
				sqsstub.receiveMessages = sinon.mock().once();

				queue.receiveMessage( function() {});
			})
		});

		describe("receiveMessage",function(){
			it("should call a callback on success recieve", function(){			
				var receiveMessageData = {
					Messages:[{
						messageBody: "sampleMessageId", MD5OfBody:"sampleMD5", receipeHandle: "sampleHandle"
					}]
				}
				sqsstub.receiveMessages = sinon.mock().callsArgWith(1, null, receiveMessageData);

				queue.receiveMessage(function(err, data) {
					assert.equal(data, receiveMessageData.Messages[0], JSON.stringify(data) + " : " + JSON.stringify(receiveMessageData));
				});
			})
		});
		describe("receiveMessage",function(){
			it("should call a callback on error recv", function(){			
				var recvError = "error"
				sqsstub.receiveMessages = sinon.mock().callsArgWith(1, recvError);

				queue.receiveMessage(function(err, data) {
					assert.equal(err, recvError, err + " : " + recvError);
					assert.equal(data, undefined, data + " : " + undefined);
				});
			})
		});

		describe("receiveMessage",function(){
			it("should call a sqs.deleteMessage once on success", function(){			
				var receiveMessageData = {
					Messages:[{
						messageBody: "sampleMessageId", MD5OfBody:"sampleMD5", receipeHandle: "sampleHandle"
					}]
				}
				sqsstub.deleteMessage = sinon.mock().once();
				sqsstub.receiveMessages = sinon.mock().callsArgWith(1, null, receiveMessageData);

				queue.receiveMessage(function() {});
			})
		});
		describe("receiveMessage",function(){
			it("should not call a sqs.deleteMessage on error", function(){			
				
				sqsstub.deleteMessage = sinon.mock().never();
				sqsstub.receiveMessages = sinon.mock().callsArgWith(1, "error");

				queue.receiveMessage(function() {});
			})
		});

});
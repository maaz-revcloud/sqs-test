const AWS = require("aws-sdk");

AWS.config.update({
  region: "us-west-2",
  signatureVersion: "v4",
});

const sqs = new AWS.SQS();

// URL of your SQS queue
const queueUrl = 'https://sqs.us-west-2.amazonaws.com/221490242148/MyMessages';

async function receiveAndDeleteMessages() {
  try {
    // Receive messages from the SQS queue
    const receiveParams = {
      QueueUrl: queueUrl,
      MaxNumberOfMessages: 10, // Receive one message at a time
      VisibilityTimeout: 30, // Set visibility timeout in seconds
      WaitTimeSeconds: 20, // Long polling to reduce request frequency
    };

    const receiveResult = await sqs.receiveMessage(receiveParams).promise();
    console.log(receiveResult)

    if (receiveResult.Messages && receiveResult.Messages.length > 0) {
      const message = receiveResult.Messages[0];
      console.log('Received Message:', message);

      // Your logic to process the message goes here

      // Delete the message from the SQS queue
      const deleteParams = {
        QueueUrl: queueUrl,
        ReceiptHandle: message.ReceiptHandle,
      };

      await sqs.deleteMessage(deleteParams).promise();
      console.log('Deleted Message:', message.MessageId);
    } else {
      console.log('No messages available in the queue.');
    }
  } catch (error) {
    console.error('Error:', error);
  }
}

// Call the function to receive and delete messages
receiveAndDeleteMessages();
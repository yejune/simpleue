<?php
/**
 * User: Javier Bravo
 * Date: 10/05/15
 */
namespace Simpleue\Queue;

use Aws\Sqs\Exception\SqsException;
use Aws\Sqs\SqsClient;

/*
 * AWS API 3.x doc : http://docs.aws.amazon.com/aws-sdk-php/v3/api/
 */
class Sqs implements \Simpleue\Queue
{
    public $currentJob;
    private $sqsClient;
    private $sourceQueueUrl;
    private $failedQueueUrl;
    private $errorQueueUrl;
    private $maxWaitingSeconds;
    private $visibilityTimeout;
    private $idempotentDb;

    public function __construct(\Aws\Sdk $aws, $queueName, $maxWaitingSeconds = 20, $visibilityTimeout = 43200)
    {
        $this->idempotentDb      = new Sqs\Idempotent($aws, 'supervolt', 'messageId');
        $this->sqsClient         = $aws->createSqs();
        $this->maxWaitingSeconds = $maxWaitingSeconds;
        $this->visibilityTimeout = $visibilityTimeout;
        $this->setQueues($queueName);
    }

    public function setVisibilityTimeout($visibilityTimeout)
    {
        $this->visibilityTimeout = $visibilityTimeout;

        return $this;
    }

    public function setMaxWaitingSeconds($maxWaitingSeconds)
    {
        $this->maxWaitingSeconds = $maxWaitingSeconds;

        return $this;
    }

    public function setSourceQueueUrl($queueUrl)
    {
        $this->sourceQueueUrl = $queueUrl;

        return $this;
    }

    public function setFailedQueueUrl($queueUrl)
    {
        $this->failedQueueUrl = $queueUrl;

        return $this;
    }

    public function setErrorQueueUrl($queueUrl)
    {
        $this->errorQueueUrl = $queueUrl;

        return $this;
    }

    public function setSqsClient(SqsClient $sqsClient)
    {
        $this->sqsClient = $sqsClient;

        return $this;
    }
    public function getNext()
    {
        $queueItem = $this->sqsClient->receiveMessage([
            'QueueUrl'            => $this->sourceQueueUrl,
            'MaxNumberOfMessages' => 1,
            'WaitTimeSeconds'     => $this->maxWaitingSeconds,
            'VisibilityTimeout'   => $this->visibilityTimeout,
        ]);

        if ($queueItem->hasKey('Messages')) {
            $this->currentJob = $queueItem->get('Messages')[0];
            if (false == $this->idempotentDb->get($this->currentJob['MessageId'])) {
                return $this->currentJob;
            }
            //$this->deleteMessage($this->sourceQueueUrl, $this->currentJob['ReceiptHandle'], $this->currentJob['MessageId']);
        }

        return false;
    }

    public function successful($job)
    {
        $this->deleteMessage($this->sourceQueueUrl, $job['ReceiptHandle'], $job['MessageId']);
    }

    public function failed($job)
    {
        $this->sendMessage($this->failedQueueUrl, $job['Body']);
        $this->deleteMessage($this->sourceQueueUrl, $job['ReceiptHandle'], $job['MessageId']);
    }

    public function error($job)
    {
        $this->sendMessage($this->errorQueueUrl, $job['Body']);
        $this->deleteMessage($this->sourceQueueUrl, $job['ReceiptHandle'], $job['MessageId']);
    }

    public function nothingToDo()
    {
    }

    public function stopped($job)
    {
        $this->deleteMessage($this->sourceQueueUrl, $job['ReceiptHandle'], $job['MessageId']);
    }

    public function getMessageBody($job)
    {
        return json_decode($job['Body'], true);
    }

    public function toString($body)
    {
        return json_encode($body);
    }

    public function sendJob($body)
    {
        return $this->sendMessage($this->sourceQueueUrl, $body);
    }

    protected function setQueues($queueName)
    {
        $this->sourceQueueUrl = $this->getQueueUrl($queueName.'-queue');
        $this->failedQueueUrl = $this->getQueueUrl($queueName.'-failed');
        $this->errorQueueUrl  = $this->getQueueUrl($queueName.'-error');
    }

    protected function getQueueUrl($queueName)
    {
        try {
            $queueData = $this->sqsClient->createQueue([
                'QueueName'  => $queueName,
                'Attributes' => [
                    'VisibilityTimeout' => $this->visibilityTimeout,
                ],
            ]);
            /*
            $queueData = $this->sqsClient->getQueueUrl(['QueueName' => $queueName]);
            */
        } catch (SqsException $ex) {
            throw $ex;
        }

        return $queueData->get('QueueUrl');
    }

    protected function deleteMessage($queueUrl, $messageReceiptHandle, $messageId)
    {
        $this->idempotentDb->update($messageId, 1);
        $this->sqsClient->deleteMessage([
            'QueueUrl'      => $queueUrl,
            'ReceiptHandle' => $messageReceiptHandle,
        ]);
    }

    private function sendMessage($queueUrl, $messageBody)
    {
        return $this->sqsClient->sendMessage([
            'QueueUrl'    => $queueUrl,
            'MessageBody' => json_encode($messageBody),
        ]);
    }
}

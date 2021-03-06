<?php
/**
 * User: Javier Bravo
 * Date: 10/05/15
 */
namespace Simpleue\Queue;

use Predis\Client;

class Redis implements \Simpleue\Queue
{
    private $redisClient;
    private $sourceQueue;
    private $maxWaitingSeconds;

    public function __construct(Client $redisClient, $queueName, $maxWaitingSeconds = 30)
    {
        $this->redisClient       = $redisClient;
        $this->sourceQueue       = $queueName;
        $this->maxWaitingSeconds = $maxWaitingSeconds;
    }

    public function setRedisClient(Client $redisClient)
    {
        $this->redisClient = $redisClient;

        return $this;
    }

    public function setQueueName($queueName)
    {
        $this->sourceQueue = $queueName;

        return $this;
    }

    public function setMaxWaitingSeconds($maxWaitingSeconds)
    {
        $this->maxWaitingSeconds = $maxWaitingSeconds;

        return $this;
    }

    public function getNext()
    {
        $queueItem = $this->redisClient->brpoplpush($this->getSourceQueue(), $this->getProcessingQueue(), $this->maxWaitingSeconds);

        return ($queueItem !== null) ? $queueItem : false;
    }

    public function successful($job)
    {
        $this->redisClient->lrem($this->getProcessingQueue(), 1, $job);
    }

    public function failed($job)
    {
        $this->redisClient->lpush($this->getFailedQueue(), $job);
        $this->redisClient->lrem($this->getProcessingQueue(), 1, $job);
    }

    public function error($job)
    {
        $this->redisClient->lpush($this->getErrorQueue(), $job);
        $this->redisClient->lrem($this->getProcessingQueue(), 1, $job);
    }

    public function nothingToDo()
    {
        $this->redisClient->ping();
    }

    public function stopped($job)
    {
        $this->redisClient->lrem($this->getProcessingQueue(), 1, $job);
    }

    public function getMessageBody($job)
    {
        return json_decode($job, true);
    }

    public function toString($job)
    {
        return $job;
    }

    public function sendJob($job)
    {
        return $this->redisClient->lpush($this->getSourceQueue(), json_encode($job));
    }

    protected function getSourceQueue()
    {
        return $this->sourceQueue.'-source';
    }

    protected function getProcessingQueue()
    {
        return $this->sourceQueue.'-processing';
    }

    protected function getFailedQueue()
    {
        return $this->sourceQueue.'-failed';
    }

    protected function getErrorQueue()
    {
        return $this->sourceQueue.'-error';
    }
}

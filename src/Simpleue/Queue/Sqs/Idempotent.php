<?php
namespace Simpleue\Queue;

use Aws\DynamoDb\DynamoDbClient;

class Idempotent
{
    /**
     * @var DynamoDbClient
     */
    private $dynamoDbClient;
    /**
     * @var string
     */
    private $tableName;
    /**
     * @var string
     */
    private $attribute;
    /**
     * StoreKeyClient constructor.
     *
     * @param DynamoDbClient $dynamoDbClient
     * @param string         $tableName
     * @param string         $attribute
     */
    public function __construct(\Aws\Sdk $aws, $tableName, $attribute)
    {
        $this->dynamoDbClient = $aws->createDynamoDb();
        $this->tableName      = $tableName;
        $this->attribute      = $attribute;
    }
    public function update($messageId, $add)
    {
        if ($item = $this->getValue($messageId)) {
            $this->put($messageId, $item + $add);
        } else {
            $this->put($messageId, $add);
        }
    }
    /**
     * @param string $key
     * @param mixed  $value
     */
    public function put($key, $value)
    {
        $this->dynamoDbClient->putItem([
            'TableName' => $this->tableName,
            'Item'      => [
                $this->attribute => [$this->getVariableType($key) => (string) $key],
                'status'         => [$this->getVariableType($value) => (string) $value],
            ],
        ]);
    }
    /**
     * @deprecated deprecated since version 2.0
     *
     * @param string $key
     *
     * @returns mixed
     */
    public function get($key)
    {
        $response = $this->dynamoDbClient->getItem([
            'TableName' => $this->tableName,
            'Key'       => [
                $this->attribute => [$this->getVariableType($key) => (string) $key],
            ],
        ]);
        if ($response->hasKey('Item')) {
            return $response['Item'];
        }

        return false;
    }
    /**
     * @param $key
     ** get and getValue methods return false on non existent key* get and getValue methods return false on non existent key
     * @return int|string|bool
     */
    public function getValue($key)
    {
        $response = $this->dynamoDbClient->getItem([
            'TableName' => $this->tableName,
            'Key'       => [
                $this->attribute => [$this->getVariableType($key) => (string) $key],
            ],
        ]);
        if ($response->hasKey('Item')) {
            $array = $response['Item']['status'];
            $value = reset($array);
            $key   = key($array);

            return $this->getCastedResponse($key, $value);
        }

        return false;
    }
    /**
     * @param $key
     */
    public function delete($key)
    {
        $this->dynamoDbClient->deleteItem([
            'TableName' => $this->tableName,
            'Key'       => [
                $this->attribute => [$this->getVariableType($key) => (string) $key],
            ],
        ]);
    }
    /**
     * @param $value
     *
     * @return string
     */
    private function getVariableType($value)
    {
        switch (gettype($value)) {
            case 'integer':
                return 'N';
            default:
                return 'S';
        }
    }
    /**
     * @param $key
     * @param $value
     *
     * @return int|string
     */
    private function getCastedResponse($key, $value)
    {
        if ($key === 'N') {
            return (int) $value;
        }

        return (string) $value;
    }
}

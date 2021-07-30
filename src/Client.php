<?php

namespace iCraftLtd\Component\Etcd;

use RuntimeException;
use GuzzleHttp\Client as Guzzle;
use GuzzleHttp\Exception\BadResponseException;
use iCraftLtd\Component\Etcd\Exception\Exception;
use iCraftLtd\Component\Etcd\Exception\KeyExistsException;
use iCraftLtd\Component\Etcd\Exception\KeyNotFoundException;

/**
 * Class Client
 * @package iCraftLtd\Component\Etcd
 */
class Client
{
    /**
     * @var string
     */
    private $server;
    /**
     * @var Guzzle
     */
    private $guzzle;
    /**
     * @var
     */
    private $apiVersion;
    /**
     * @var string
     */
    private $namespace = '';

    /**
     * Client constructor.
     *
     * @param string $server
     * @param string|null $namespace
     * @param string $version
     */
    public function __construct(string $server = 'http://127.0.0.1:2379', string $namespace = null, string $version = 'v2')
	{
		if ($server = rtrim($server, '/')) {
			$this->server = $server;
		}

		$this->namespace = $namespace ?: '/';
		$this->apiVersion = $version;
        $this->guzzle = new Guzzle(['base_uri' => $this->server]);
	}

	/**
	 * Set custom guzzle in Client
	 * 
	 * @param Guzzle $guzzle
	 * @return Client
	 */
	public function setGuzzle(Guzzle $guzzle): Client
    {
		$this->guzzle = $guzzle;

		return $this;
    }

    /**
     * Set the default root directory. the default is `/`
     * If the root is others e.g. /linkorb when you set new key,
     * or set dir, all of the key is under the root
     * e.g.
     * <code>
     *    $client->setRoot('/linkorb');
     *    $client->set('key1, 'value1');
     *    // the new key is /linkorb/key1
     * </code>
     * @param string $namespace
     * @return Client
     */
    public function setNamespace(string $namespace): Client
    {
        if (strpos($namespace, '/') !== 0) {
            $namespace = '/' . $namespace;
        }
        $this->namespace = rtrim($namespace, '/');

        return $this;
    }

    /**
     * Build key space operations
     * 
     * @param string $key
     * @return string
     */
    private function buildKeyUri(string $key): string
    {
        if (strpos($key, '/') !== 0) {
            $key = '/' . $key;
        }

        return '/' . $this->apiVersion . '/keys' . $this->root . $key;
    }

    /**
     * Get server version
     * 
     * @param string $uri
     * @return mixed
     */
    public function getVersion(string $uri)
    {
        $response = $this->guzzle->get($uri);
        $data = json_decode($response->getBody(), true);
        if (JSON_ERROR_NONE !== json_last_error()) {
            throw new RuntimeException('Unable to parse response body into JSON: ' . json_last_error());
        }

        return $data;
    }

    /**
     * Set the value of a key
     *
     * @param string $key
     * @param $value
     * @param int|null $ttl
     * @param array $condition
     * @return stdClass
     */
    public function set(string $key, $value, ?int $ttl = null, array $condition = array())
    {
        $data = array('value' => $value);

        if ($ttl) {
            $data['ttl'] = $ttl;
        }

        try {
            $response = $this->guzzle->put($this->buildKeyUri($key), array(
                'query' => $condition,
                'form_params' => $data
            ));
        } catch (BadResponseException $e) {
            $response = $e->getResponse();
        }

        $body = json_decode($response->getBody(), true);
        if (JSON_ERROR_NONE !== json_last_error()) {
            throw new \RuntimeException('Unable to parse response body into JSON: ' . json_last_error());
        }

        return $body;
    }

    /**
     * Retrieve the value of a key
     * 
     * @param string $key
     * @param array $query the extra query params
     * @return array|void
     * @throws KeyNotFoundException
     */
    public function getNode(string $key, array $query = array())
    {
        try {
            $response = $this->guzzle->get(
                $this->buildKeyUri($key),
                array(
                    'query' => $query
                )
            );
        } catch (BadResponseException $e) {
            $response = $e->getResponse();
        }

        $body = json_decode($response->getBody(), true);
        if (JSON_ERROR_NONE !== json_last_error()) {
            throw new RuntimeException('Unable to parse response body into JSON: ' . json_last_error());
        }

        if (isset($body['errorCode'])) {
            throw new KeyNotFoundException($body['message'], $body['errorCode']);
        }

        return $body['node'];
    }

    /**
     * Retrieve the value of a key
     * 
     * @param string $key
     * @param array $flags the extra query params
     * @return string|void the value of the key.
     * @throws KeyNotFoundException
     */
    public function get(string $key, array $flags = array())
    {
        try {
            $node = $this->getNode($key, $flags);
            return $node['value'];
        } catch (KeyNotFoundException $ex) {
            throw $ex;
        }
    }

    /**
     * Make a new key with a given value
     *
     * @param string $key
     * @param $value
     * @param int $ttl
     * @return stdClass
     * @throws KeyExistsException
     */
    public function mk(string $key, $value, int $ttl = 0)
    {
        $body = $request = $this->set(
            $key,
            $value,
            $ttl,
            array('prevExist' => 'false')
        );

        if (isset($body['errorCode'])) {
            throw new KeyExistsException($body['message'], $body['errorCode']);
        }

        return $body;
    }

    /**
     * Make a new directory
     *
     * @param string $key
     * @param int $ttl
     * @return array | void
     * @throws KeyExistsException
     */
    public function mkdir(string $key, int $ttl = 0)
    {
        $data = array('dir' => 'true');

        if ($ttl) {
            $data['ttl'] = $ttl;
        }

        try {
            $response = $this->guzzle->put(
                $this->buildKeyUri($key),
                array(
                    'query' => array('prevExist' => 'false'),
                    'form_params' => $data
                )
            );
        } catch (BadResponseException $e) {
            $response = $e->getResponse();
        }

        $body = json_decode($response->getBody(), true);
        if (JSON_ERROR_NONE !== json_last_error()) {
            throw new RuntimeException('Unable to parse response body into JSON: ' . json_last_error());
        }

        if (isset($body['errorCode'])) {
            throw new KeyExistsException($body['message'], $body['errorCode']);
        }

        return $body;
    }


    /**
     * Update an existing key with a given value.
     * 
     * @param string $key
     * @param $value
     * @param int $ttl
     * @param array $condition The extra condition for updating
     * @return array | void
     * @throws KeyNotFoundException
     */
    public function update(string $key, $value, int $ttl = 0, $condition = array())
    {
        $extra = array('prevExist' => 'true');

        if ($condition) {
            $extra = array_merge($extra, $condition);
        }

        $body = $this->set($key, $value, $ttl, $extra);
        if (isset($body['errorCode'])) {
            throw new KeyNotFoundException($body['message'], $body['errorCode']);
        }

        return $body;
    }

    /**
     * Update directory
     * 
     * @param string $key
     * @param int $ttl
     * @return array | void
     * @throws Exception
     */
    public function updateDir(string $key, int $ttl)
    {
        if (!$ttl) {
            throw new Exception('TTL is required', 204);
        }

        $condition = array(
            'dir' => 'true',
            'prevExist' => 'true'
        );

        $response = $this->guzzle->put(
            $this->buildKeyUri($key),
            array(
                'query' => $condition,
                'form_params' => array(
                    'ttl' => (int)$ttl
                )
            )
        );

        $body = json_decode($response->getBody(), true);
        if (JSON_ERROR_NONE !== json_last_error()) {
            throw new RuntimeException('Unable to parse response body into JSON: ' . json_last_error());
        }

        if (isset($body['errorCode'])) {
            throw new Exception($body['message'], $body['errorCode']);
        }

        return $body;
    }


    /**
     * Remove a key
     * 
     * @param string $key
     * @return array|stdClass
     * @throws Exception
     */
    public function rm(string $key)
    {
        try {
            $response = $this->guzzle->delete($this->buildKeyUri($key));
        } catch (BadResponseException $e) {
            $response = $e->getResponse();
        }

        $body = json_decode($response->getBody(), true);
        if (JSON_ERROR_NONE !== json_last_error()) {
            throw new RuntimeException('Unable to parse response body into JSON: ' . json_last_error());
        }

        if (isset($body['errorCode'])) {
            throw new Exception($body['message'], $body['errorCode']);
        }

        return $body;
    }

    /**
     * Removes the key if it is directory
     * 
     * @param string $key
     * @param boolean $recursive
     * @return mixed
     * @throws Exception
     */
    public function rmdir(string $key, bool $recursive = false)
    {
        $query = array('dir' => 'true');

        if ($recursive === true) {
            $query['recursive'] = 'true';
        }

        try {
            $response = $this->guzzle->delete(
                $this->buildKeyUri($key),
                array(
                    'query' => $query
                )
            );
        } catch (BadResponseException $e) {
            $response = $e->getResponse();
        }

        $body = json_decode($response->getBody(), true);
        if (JSON_ERROR_NONE !== json_last_error()) {
            throw new RuntimeException('Unable to parse response body into JSON: ' . json_last_error());
        }

        if (isset($body['errorCode'])) {
            throw new Exception($body['message'], $body['errorCode']);
        }

        return $body;
    }

    /**
     * Retrieve a directory
     * 
     * @param string $key
     * @param boolean $recursive
     * @return mixed
     * @throws KeyNotFoundException
     */
    public function listDir(string $key = '/', bool $recursive = false)
    {
        $query = array();
        if ($recursive === true) {
            $query['recursive'] = 'true';
        }
        $response = $this->guzzle->get(
            $this->buildKeyUri($key),
            array(
                'query' => $query
            )
        );

        $body = json_decode($response->getBody(), true);
        if (JSON_ERROR_NONE !== json_last_error()) {
            throw new RuntimeException('Unable to parse response body into JSON: ' . json_last_error());
        }

        if (isset($body['errorCode'])) {
            throw new KeyNotFoundException($body['message'], $body['errorCode']);
        }

        return $body;
    }

    /**
     * Retrieve a directories key
     * 
     * @param string $key
     * @param boolean $recursive
     * @return array
     * @throws Exception
     */
    public function ls(string $key = '/', bool $recursive = false)
    {
        $this->values = array();
        $this->dirs = array();

        try {
            $data = $this->listDir($key, $recursive);
        } catch (Exception $e) {
            throw $e;
        }

        return $this->traversalDir(new RecursiveArrayIterator($data));
    }

    /**
     * @var array
     */
    private $dirs = array();

    /**
     * @var array
     */
    private $values = array();


    /**
     * Traversal the directory to get the keys.
     * 
     * @param RecursiveArrayIterator $iterator
     * @return array
     */
    private function traversalDir(RecursiveArrayIterator $iterator)
    {
        $key = '';
        while ($iterator->valid()) {
            if ($iterator->hasChildren()) {
                $this->traversalDir($iterator->getChildren());
            } else {
                if ($iterator->key() == 'key' && ($iterator->current() != '/')) {
                    $this->dirs[] = $key = $iterator->current();
                }

                if ($iterator->key() == 'value') {
                    $this->values[$key] = $iterator->current();
                }
            }

            $iterator->next();
        }

        return $this->dirs;
    }

    /**
     * Get all key-value pair that the key is not directory.
     * 
     * @param string $key
     * @param boolean $recursive
     * @param string $key
     * @return array
     */
    public function getKeysValue(string $root = '/', bool $recursive = true, ?string $key = null)
    {
        $this->ls($root, $recursive);
        if (isset($this->values[$key])) {
            return $this->values[$key];
        }

        return $this->values;
    }

    /**
     * Create a new directory with auto generated id
     *
     * @param string $dir
     * @param int $ttl
     * @return array $body
     */
    public function mkdirWithInOrderKey(string $dir, int $ttl = 0)
    {
        $data = array(
            'dir' => 'true'
        );

        if ($ttl) {
            $data['ttl'] = $ttl;
        }

        $request = $this->guzzle->post(
            $this->buildKeyUri($dir),
            null,
            $data
        );

        $response = $request->send();
        $body = $response->json();

        return $body;
    }

    /**
     * Create a new key in a directory with auto generated id
     *
     * @param string $dir
     * @param $value
     * @param int $ttl
     * @param array $condition
     * @return array $body
     */
    public function setWithInOrderKey(string $dir, $value, int $ttl = 0, array $condition = array())
    {
        $data = array('value' => $value);

        if ($ttl) {
            $data['ttl'] = $ttl;
        }

        $request = $this->guzzle->post($this->buildKeyUri($dir), null, $data, array(
            'query' => $condition
        ));

        $response = $request->send();

        return $response->json();
    }
}
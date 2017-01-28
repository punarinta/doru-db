<?php

namespace DoruDB;

class Driver
{
    /**
     * Internal storage object
     *
     * @var Storage
     */
    private $storage;

    /**
     * Driver constructor.
     *
     * @param string $dir
     */
    public function __construct($dir = 'db')
    {
        $this->storage = new Storage($dir);
    }

    /**
     * Generates a random ID
     *
     * @return string
     */
    public function id()
    {
        return sprintf('%04X%04X%08X', crc32(gethostname()) & 0xFFFF, (microtime(true) * 1000) & 0xFFFF, mt_rand(0, 4294967295));
    }

    /**
     * Creates a document
     *
     * @param $collection
     * @param array $object
     * @return mixed
     * @throws \Exception
     */
    public function create($collection, $object = [])
    {
        if (!$collection)
        {
            throw new \Exception('Collection not specified');
        }

        $object = (object) $object;

        $object->id = isset ($object->id) ? $object->id : $this->id();

        return $this->update($collection, $object);
    }

    /**
     * Updates a document. Always upserts.
     *
     * @param $collection
     * @param $object
     * @return mixed
     * @throws \Exception
     */
    public function update($collection, $object)
    {
        if (!$collection)
        {
            throw new \Exception('Collection not specified');
        }

        if (!$object)
        {
            throw new \Exception('Object not specified');
        }

        if (!isset ($object->id) || !$object->id)
        {
            throw new \Exception('Object does not have an ID');
        }

        $this->storage->assureCollection($collection);
        $this->storage->write($collection . '/' .  $object->id, $object);

        return $object;
    }

    /**
     * Removes a document
     *
     * @param $collection
     * @param $id
     * @return bool
     * @throws \Exception
     */
    public function delete($collection, $id)
    {
        if (!$collection)
        {
            throw new \Exception('Collection not specified');
        }

        if (!$id)
        {
            throw new \Exception('Object (or its ID) not specified');
        }

        if (!is_string($id))
        {
            $id = $id->id;
        }

        return $this->storage->remove($collection . '/' .  $id);
    }

    /**
     * Reads a single document found by its ID
     *
     * @param $collection
     * @param $id
     * @return array|bool|null
     * @throws \Exception
     */
    public function findById($collection, $id)
    {
        if (!$collection)
        {
            throw new \Exception('Collection not specified');
        }

        if (!$id)
        {
            throw new \Exception('Object ID not specified');
        }

        return $this->storage->read($collection . '/' .  $id);
    }
}

<?php

namespace DoruDB;

class Database
{
    /**
     * Internal storage object
     *
     * @var Storage
     */
    private $storage;

    /**
     * List of auto-incremented indices for collections
     *
     * @var array
     */
    private $autoIds = [];

    /**
     * Driver constructor.
     *
     * @param string $dir
     */
    public function __construct($dir = 'db')
    {
        $this->storage = new Storage($dir);

        if (file_exists($dir)) foreach (scandir($dir) as $collection)
        {
            if ($collection[0] == '.') continue;

            if (count($files = scandir($dir . '/' . $collection)) > 2)
            {
                $this->autoIds[$collection] = end($files);
            }
        }
    }

    /**
     * Generates a random ID
     *
     * @param $collection
     * @return string
     */
    public function id($collection)
    {
        if (!isset ($this->autoIds[$collection]))
        {
            $this->autoIds[$collection] = 0;
        }

        return ++$this->autoIds[$collection];
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

        $object->id = isset ($object->id) ? $object->id : $this->id($collection);

        $this->storage->assureCollection($collection);
        $this->storage->write($collection . '/' .  sprintf('%08X', $object->id), $object);

        return $object;
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

        return unlink($this->storage->path() . $collection . '/' .  $id);
    }

    /**
     * Finds a document by its ID
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

    /**
     * Finds all the documents of a specific type
     *
     * @param $collection
     * @param array $filter
     * @return array
     * @throws \Exception
     */
    public function findAll($collection, $filter = [])
    {
        if (!$collection)
        {
            throw new \Exception('Collection not specified');
        }

        $rows = [];
        $files = scandir($dir = $this->storage->path() . $collection . '/');

        foreach (array_slice($files, 2) as $file)
        {
            $row = $this->storage->read($collection . '/' .  $file);

            if ($filter) foreach ($filter as $k => $v)
            {
                if (!isset ($row->{$k}) || $row->{$k} != $v) goto skip_row;
            }

            $rows[] = $row;

            skip_row:;
        }

        return $rows;
    }
}

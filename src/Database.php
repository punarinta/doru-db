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

        $object->id = $object->id ?? $this->id($collection);

        $this->storage->assureCollection($collection);
        $this->storage->write($collection . '/' .  sprintf('%010d', $object->id), $object);

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
     * Find one specific document
     *
     * @param $collection
     * @param array $setup
     * @return mixed|null
     */
    public function find($collection, $setup = [])
    {
        $setup['limit'] = 1;

        $rows = $this->findAll($collection, $setup);

        return $rows ? $rows[0] : null;
    }

    /**
     * Finds all specific documents
     *
     * @param $collection
     * @param array $setup
     * @return array
     * @throws \Exception
     */
    public function findAll($collection, $setup = [])
    {
        if (!$collection)
        {
            throw new \Exception('Collection not specified');
        }

        $limit = 0;
        $offset = 0;

        if ($setup['filter'] ?? 0)
        {
            // use deep scan
            $limit = $setup['limit'] ?? 0;
            $offset = $setup['offset'] ?? 0;
            unset ($setup['limit']);
            unset ($setup['offset']);
        }

        $rows = [];
        $count = 0;
        $files = $this->getIndexedList($collection, $setup);

        foreach ($files as $file)
        {
            ++$count;

            if ($offset && $count <= $offset) continue;

            $row = $this->storage->read($collection . '/' .  $file);

            foreach ($setup['filter'] ?? [] as $k => $v)
            {
                if (is_callable($v))
                {
                    if (!$v($row->{$k})) goto skip_row;
                }
                else
                {
                    if ($row->{$k} ?? null != $v) goto skip_row;
                }
            }

            if ($limit && $count > $offset + $limit) continue;

            $rows[] = $row;

            skip_row:;
        }

        return $rows;
    }

    /**
     * Count specific documents
     *
     * @param $collection
     * @param $setup
     * @return int
     */
    public function count($collection, $setup = [])
    {
        // simply return the size of the prepared indexed list

        return count($this->getIndexedList($collection, $setup));
    }

    /**
     * Creates a list of documents based on setup
     *
     * @param $collection
     * @param array $setup
     * @return array
     */
    private function getIndexedList($collection, $setup = [])
    {
        $invert = $setup['invert'] ?? 0;
        $files = scandir($dir = $this->storage->path() . $collection . '/', $invert);

        // remove '.' and '..' entries, apply offset and limit
        return array_slice($files, ($invert ? 0 : 2) + ($setup['offset'] ?? 0), $setup['limit'] ?? null);

        // TODO: work with custom indices here
    }
}

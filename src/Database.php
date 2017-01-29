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
     * List of available indices
     *
     * @var array
     */
    private $indices = [];

    /**
     * Driver constructor.
     *
     * @param string $dir
     */
    public function __construct($dir = 'db')
    {
        $this->storage = new Storage($dir);

        if (file_exists($dir)) foreach (scandir($dir) as $object)
        {
            if (strpos($object, '.') !== false)
            {
                // is either a unix dot dir or an index
                if ($object[0] == '.') continue;

                $object = explode('.', $object);
                $this->indices[$object[0]][$object[1]] = 1;

                continue;
            }

            if (count($files = scandir($dir . '/' . $object)) > 2)
            {
                $this->autoIds[$object] = end($files);
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
     * Removes the whole documents collection
     *
     * @param $collection
     * @return bool
     */
    public function truncate($collection)
    {
        $dir = $this->storage->path() . $collection;
        array_map('unlink', glob("$dir/*"));

        return rmdir($dir);
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
        $explicitIndex = null;

        if ($filter = $setup['filter'] ?? 0)
        {
            if (count($filter) == 1 && isset ($this->indices[$collection][key($filter)]))
            {
                // index is available for this key
                $explicitIndex = key($filter);
            }
            else
            {
                // we have to run full scan
                $limit = $setup['limit'] ?? 0;
                $offset = $setup['offset'] ?? 0;
                unset ($setup['limit']);
                unset ($setup['offset']);
            }
        }

        $rows = [];
        $count = 0;
        $files = $this->getIndexedList($collection, $setup, $explicitIndex);

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
     * Creates a list of document IDs based on setup
     *
     * @param $collection
     * @param array $setup
     * @param string|null $explicitIndex
     * @return array
     */
    private function getIndexedList($collection, $setup = [], $explicitIndex = null)
    {
        $invert = $setup['invert'] ?? 0;

        if ($explicitIndex)
        {
            $items = [];
            $ids = json_decode(file_get_contents($this->storage->path() . $collection . '.' . $explicitIndex), 1) ?: [];

            if ($indexFilter = $setup['filter'][$explicitIndex] ?? null)
            {
                // apply filtering here
                foreach ($ids as $k => $v)
                {
                    if (is_callable($indexFilter))
                    {
                        if (!$indexFilter($k)) continue;
                    }
                    else
                    {
                        if ($k != $indexFilter) continue;
                    }
                    $items[] = sprintf('%010d', $v);
                }
            }
            else
            {
                $items = array_values($items);
            }

            if ($invert) $items = array_reverse($items);
        }
        else
        {
            $items = scandir($dir = $this->storage->path() . $collection . '/', $invert);

            // remove '.' and '..' entries, apply offset and limit
            $items = array_slice($items, $invert ? 0 : 2, count($items) - 2);
        }

        return array_slice($items, $setup['offset'] ?? 0, $setup['limit'] ?? null);
    }
}

<?php

declare(strict_types=1);

namespace DoruDB;

/**
 * Class Database
 * @package DoruDB
 */
class Database
{
    /**
     * Working directory
     *
     * @var string
     */
    private $dir;

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
     * @var Index[][]
     */
    private $indices = [];

    /**
     * If 'explain' mode is on this contains a list of explanations
     *
     * @var array
     */
    private $explanations = [];

    /**
     * Driver constructor.
     *
     * @param string $dir
     */
    public function __construct(string $dir = 'db')
    {
        $this->dir = $dir;
        $this->storage = new Storage($dir);

        if (file_exists($dir)) foreach (scandir($dir) as $object)
        {
            if (strpos($object, '.') !== false)
            {
                // is either a unix dot dir or an index
                if ($object[0] == '.') continue;

                $object = explode('.', $object);
                $this->indices[$object[0]][$object[1]] = new Index($object[0], $object[1], $dir);

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
     * @param string $collection
     * @return int
     */
    public function id(string $collection) : int
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
     * @param array $objectArray
     * @return mixed
     * @throws \Exception
     */
    public function create(string $collection, array $objectArray = []) : object
    {
        if (!$collection)
        {
            throw new \Exception('Collection not specified');
        }

        if (!preg_match('/^[\w-]+$/', $collection))
        {
            throw new \Exception('Only [a-zA-Z0-9_-] are allowed in collection names.');
        }

        $this->storage->assureCollection($collection);

        $object = (object) $objectArray;
        if (isset ($object->id))
        {
            if (!is_int($object->id))
            {
                throw new \Exception('ID must be an integer. Provided: ' . $object->id);
            }
        }
        else
        {
            $object->id = $this->id($collection);
        }

        $relativeFilename = $collection . '/' .  sprintf('%010d', $object->id);

        if (file_exists($this->storage->path() . $relativeFilename))
        {
            throw new \Exception('Cannot create a record with a duplicate ID');
        }

        $this->storage->write($relativeFilename, $object);

        return $object;
    }

    /**
     * Updates a document. Always upserts.
     *
     * @param string $collection
     * @param mixed $object
     * @return mixed
     * @throws \Exception
     */
    public function update(string $collection, object $object) : object
    {
        if (!$collection)
        {
            throw new \Exception('Empty collection name');
        }

        if (!isset ($object->id))
        {
            throw new \Exception('Object does not have an ID');
        }

        $this->storage->assureCollection($collection);
        $this->storage->write($collection . '/' .  sprintf('%010d', $object->id), $object);

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
    public function delete(string $collection, $id) : bool
    {
        if (!$collection)
        {
            throw new \Exception('Empty collection name');
        }

        if (is_object($id))
        {
            $id = $id->id;
        }
        elseif (!is_int($id))
        {
            throw new \Exception('Object (or its ID) not specified');
        }

        return unlink($this->storage->path() . $collection . '/' .  sprintf('%010d', $id));
    }

    /**
     * Removes the whole documents collection
     *
     * @param string $collection
     * @return bool
     */
    public function truncate(string $collection) : bool
    {
        $dir = $this->storage->path() . $collection;
        array_map('unlink', glob("$dir/*"));

        return rmdir($dir);
    }

    /**
     * Finds a document by its ID
     *
     * @param string $collection
     * @param int $id
     * @return mixed
     * @throws \Exception
     */
    public function findById(string $collection, int $id) : ?object
    {
        if (!$collection)
        {
            throw new \Exception('Empty collection name');
        }

        return $this->storage->read($collection . '/' .  sprintf('%010d', $id));
    }

    /**
     * Find one specific document
     *
     * @param string $collection
     * @param array $setup
     * @return mixed|null
     * @throws \Exception
     */
    public function find(string $collection, array $setup = []) : ?object
    {
        $setup['limit'] = 1;

        $rows = $this->findAll($collection, $setup);

        return $rows ? $rows[0] : null;
    }

    /**
     * Finds all specific documents
     *
     * @param string $collection
     * @param array $setup
     * @return array
     * @throws \Exception
     */
    public function findAll(string $collection, array $setup = []) : array
    {
        if (!$collection)
        {
            throw new \Exception('Empty collection name');
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
                $limit = $setup['limit'] ?? null;
                $offset = $setup['offset'] ?? 0;
                unset ($setup['limit']);
                unset ($setup['offset']);
            }
        }

        if (isset ($setup['explain']))
        {
            $this->explanations = [$explicitIndex ? "Index '$explicitIndex' used" : 'No index used'];
        }

        $rows = [];
        $count = 0;
        $files = $this->getIndexedList($collection, $setup, $explicitIndex);

        foreach ($files as $file)
        {
            ++$count;

            if ($count <= $offset) continue;

            $row = $this->storage->read($collection . '/' .  $file);

            foreach ($setup['filter'] ?? [] as $k => $v)
            {
                if (is_callable($v))
                {
                    if (!$v($row->{$k})) goto skip_row;
                }
                else
                {
                    if (($row->{$k} ?? null) != $v) goto skip_row;
                }
            }

            if ($limit && $count > $offset + $limit) break;

            $rows[] = $row;

            skip_row:;
        }

        return $rows;
    }

    /**
     * Count specific documents
     *
     * @param string $collection
     * @param array $setup
     * @return int
     * @throws \Exception
     */
    public function count(string $collection, array $setup = []) : int
    {
        $explicitIndex = null;
        $setup['limit'] = null;
        $setup['offset'] = 0;

        if ($filter = $setup['filter'] ?? 0)
        {
            if (count($filter) == 1 && isset ($this->indices[$collection][key($filter)]))
            {
                // index is available for this key
                $explicitIndex = key($filter);

                if (isset ($setup['explain']))
                {
                    $this->explanations = ["Index '$explicitIndex' used"];
                }
            }
            else
            {
                $count = 0;

                if (isset ($setup['explain']))
                {
                    $this->explanations = ['No index used'];
                }

                // iterate through all the documents and check filters (slooow)
                foreach ($this->findAll($collection, $setup) as $row)
                {
                    foreach ($filter as $k => $v)
                    {
                        if (is_callable($v))
                        {
                            if ($v($row->{$k})) { ++$count; break; }
                        }
                        else
                        {
                            if (($row->{$k} ?? null) == $v) { ++$count; break; }
                        }
                    }
                }

                return $count;
            }
        }

        // simply return the size of the prepared indexed list
        return count($this->getIndexedList($collection, $setup, $explicitIndex));
    }

    /**
     * Builds or rebuilds an index. Returns index size.
     *
     * @param string $collection
     * @param string $field
     * @param array $options
     * @return int
     * @throws \Exception
     */
    public function rebuildIndex(string $collection, string $field, array $options = []) : int
    {
        if (!file_exists($indexFile = $this->dir . '/' . $collection . '.' . $field))
        {
            if (!file_exists($this->dir)) mkdir($this->dir, 0700, true);
            file_put_contents($indexFile, '[]');
            $this->indices[$collection][$field] = new Index($collection, $field, $this->dir, $options);
        }
        else if (!isset ($this->indices[$collection][$field]))
        {
            $this->indices[$collection][$field] = new Index($collection, $field, $this->dir, $options);
        }

        $docs = [];

        foreach (array_slice(scandir($this->dir . '/' . $collection . '/'), 2) as $file)
        {
            $docs[] = $this->storage->read($collection . '/' .  $file);
        }

        $this->indices[$collection][$field]->update($docs);

        return count($docs);
    }

    /**
     * Updates an index
     *
     * @param string $collection
     * @param string $field
     * @param mixed $doc
     * @throws \Exception
     */
    public function updateIndex(string $collection, string $field, object $doc) : void
    {
        if (isset ($this->indices[$collection][$field]))
        {
            $this->indices[$collection][$field]->update($doc);
        }
    }

    /***
     * Removes an existing index
     *
     * @param string $collection
     * @param string $field
     */
    public function removeIndex(string $collection, string $field) : void
    {
        @unlink($this->dir . '/' . $collection . '.' . $field);
        unset ($this->indices[$collection][$field]);
    }

    /**
     * Returns or displays all the present explanations
     *
     * @param bool $print
     * @return array
     */
    public function explain(bool $print = false) : array
    {
        if ($print)
        {
            $echo = "Explain:\n--------\n";
            foreach ($this->explanations as $e) $echo .= "* $e\n";
            echo $echo . "\n";
        }

        return $this->explanations;
    }

    /**
     * Creates a list of document IDs based on setup
     *
     * @param string $collection
     * @param array $setup
     * @param string|null $explicitIndex
     * @return array
     */
    private function getIndexedList(string $collection, array $setup = [], string $explicitIndex = null) : array
    {
        $invert = $setup['invert'] ?? 0;

        if ($explicitIndex)
        {
            $items = $this->indices[$collection][$explicitIndex]->getList($setup);
        }
        else
        {
            if (!$items = @scandir($dir = $this->storage->path() . $collection . '/', $invert))
            {
                return [];
            }

            // remove '.' and '..' entries, apply offset and limit
            $items = array_slice($items, $invert ? 0 : 2, count($items) - 2);
        }

        return array_slice($items, $setup['offset'] ?? 0, $setup['limit'] ?? null);
    }
}

<?php

declare(strict_types=1);

namespace DoruDB;

/**
 * Class Index
 * Defines operations with indices
 *
 * @package DoruDB
 */
class Index
{
    /**
     * Index filename
     *
     * @var string
     */
    private $filename;

    /**
     * Attached field name
     *
     * @var string
     */
    private $field;

    /**
     * Internal key-value storage
     *
     * @var array
     */
    private $kv = null;

    /**
     * Index settings
     *
     * @var array
     */
    private $options = null;

    /**
     * Index constructor.
     *
     * @param $collection
     * @param $field
     * @param string $dir
     * @param array $options
     */
    public function __construct(string $collection, string $field, string $dir = 'db', array $options = [])
    {
        $this->field = $field;
        $this->options = $options;
        $this->filename = $dir . '/' . $collection . '.' . $field;
    }

    /**
     * Creates a list of document IDs based on setup
     *
     * @param array $setup
     * @return array
     */
    public function getList(array $setup = []) : array
    {
        $index = json_decode(@file_get_contents($this->filename)) ?: [];

        $ids = $index->kv ?? [];
        if (!$this->options) $this->options = $index->options ?? [];

        // save for next usage
        $this->kv = $ids;

        $items = [];

        if ($indexFilter = $setup['filter'][$this->field] ?? null)
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

                foreach (is_array($v) ? $v : [$v] as $id)
                {
                    $items[] = sprintf('%010d', $id);
                }
            }
        }
        else
        {
            $items = array_values($items);
        }

        if ($setup['invert'] ?? 0)
        {
            $items = array_reverse($items);
        }

        return $items;
    }

    /**
     * Updates an index
     *
     * @param array|mixed $documents
     * @return bool
     * @throws \Exception
     */
    public function update($documents) : bool
    {
        if (!is_array($documents))
        {
            if (is_object($documents)) $documents = [$documents];
            else throw new \Exception('Input must be an object or an array of objects.');
        }

        foreach ($documents as $document)
        {
            if (!isset ($document->{$this->field}))
            {
                continue;
            }

            if (!$this->kv)
            {
                $index = json_decode(@file_get_contents($this->filename)) ?: [];
                $this->kv = $index->kv ?? [];
            }

            if (isset ($this->kv[$document->{$this->field}]))
            {
                $iV = $this->kv[$document->{$this->field}];

                if ($iV == $document->id)
                {
                    // index will not change anyway
                    continue;
                }
                else
                {
                    if (is_array($iV))
                    {
                        if (in_array($document->id, $iV)) continue;
                        else $iV[] = $document->id;
                    }
                    else $iV = [$iV, $document->id];
                }
            }
            else
            {
                $iV = $document->id;
            }

            $this->kv[$document->{$this->field}] = $iV;
        }

        ksort($this->kv);

        $json = ['kv' => $this->kv];
        if ($this->options)
        {
            $json['options'] = $this->options;
        }

        return file_put_contents($this->filename, json_encode($json)) !== false;
    }
}

<?php

namespace DoruDB;

/**
 * Class Index
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
     * @var
     */
    private $field;

    /**
     * Internal key-value storage
     *
     * @var null
     */
    private $kv = null;

    /**
     * Index settings
     *
     * @var array
     */
    private $settings = [];

    /**
     * Index constructor.
     *
     * @param $collection
     * @param $field
     * @param string $dir
     */
    public function __construct($collection, $field, $dir = 'db')
    {
        $this->field = $field;
        $this->filename = $dir . '/' . $collection . '.' . $field;
    }

    /**
     * Creates a list of document IDs based on setup
     *
     * @param array $setup
     * @return array
     */
    public function getList($setup = [])
    {
        $index = json_decode(@file_get_contents($this->filename)) ?: [];

        $ids = $index->kv ?? [];
        $this->settings = $index->settings ?? [];

        // save for next usage
        $this->kv = $ids;

        $items = [];
        $invert = $setup['invert'] ?? 0;

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

        if ($invert)
        {
            $items = array_reverse($items);
        }

        return $items;
    }

    /**
     * Updates an index
     *
     * @param $document
     * @return bool|int
     */
    public function update($document)
    {
        if (!isset ($document->{$this->field}))
        {
            return false;
        }

        if (!$this->kv)
        {
            $index = json_decode(@file_get_contents($this->filename), 1) ?: [];
            $this->kv = $index->kv ?? [];
            $this->settings = $index->settings ?? [];
        }

        if (isset ($this->kv[$document->{$this->field}]))
        {
            $iV = $this->kv[$document->{$this->field}];

            if ($iV == $document->id)
            {
                // no need to update the index
                return true;
            }
            else
            {
                if (is_array($iV))
                {
                    if (in_array($iV, $document->id)) return true;
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

        ksort($this->kv);

        return file_put_contents($this->filename, json_encode(['settings' => $this->settings, 'kv' => $this->kv]));
    }
}

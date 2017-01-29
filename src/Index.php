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
        $ids = json_decode(file_get_contents($this->filename), 1) ?: [];

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
                $items[] = sprintf('%010d', $v);
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
}

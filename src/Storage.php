<?php

namespace DoruDB;

/**
 * Class Storage
 * @package DoruDB
 */
class Storage
{
    /**
     * Path to collections
     *
     * @var string
     */
    private $path;

    /**
     * Storage constructor.
     *
     * @param string $dir
     */
    public function __construct($dir = 'db')
    {
        $this->path = $dir . '/';
    }

    /**
     * Service function to return a path
     *
     * @return string
     */
    public function path()
    {
        return $this->path;
    }

    /**
     * @param $collection
     */
    public function assureCollection($collection)
    {
        if (!file_exists($this->path . $collection))
        {
            mkdir($this->path . $collection, 0700, true);
        }
    }

    /**
     * Reads data from a document
     *
     * @param $file
     * @return object|bool|null
     */
    public function read($file)
    {
        if (!$fp = @fopen($filename = $this->path . $file, 'r'))
        {
            return null;
        }

        $attempt = 0;
        while (!flock($fp, LOCK_SH | LOCK_NB))
        {
            if (++$attempt > 50)
            {
                fclose($fp);
                return false;
            }
            usleep(20000);
        }

        $raw = fread($fp, filesize($filename));

        flock($fp, LOCK_UN);
        $data = $raw ? json_decode($raw) : null;
        fclose($fp);

        return $data ?: [];
    }

    /**
     * Writes data to a document
     *
     * @param $file
     * @param $data
     * @return bool
     */
    public function write($file, $data = null)
    {
        if (!$fp = fopen($this->path . $file, 'w'))
        {
            return false;
        }

        $attempt = 0;
        $raw = json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_NUMERIC_CHECK);

        while (!flock($fp, LOCK_EX | LOCK_NB))
        {
            if (++$attempt > 50)
            {
                fclose($fp);
                return false;
            }
            usleep(20000);
        }

        fwrite($fp, $raw, strlen($raw));
        flock($fp, LOCK_UN);
        fclose($fp);

        return true;
    }
}

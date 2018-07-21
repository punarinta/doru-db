<?php

declare(strict_types=1);

namespace DoruDB;

const READ_LOCK_WAIT = 20000;
const WRITE_LOCK_WAIT = 20000;
const READ_ATTEMPTS = 50;
const WRITE_ATTEMPTS = 50;

/**
 * Class Storage
 * Defines storage layer
 *
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
    public function __construct(string $dir = 'db')
    {
        $this->path = $dir . '/';
    }

    /**
     * Service function to return a path
     *
     * @return string
     */
    public function path() : string
    {
        return $this->path;
    }

    /**
     * Assures that collection directory exists
     *
     * @param string $collection
     */
    public function assureCollection(string $collection) : void
    {
        if (!file_exists($this->path . $collection))
        {
            mkdir($this->path . $collection, 0700, true);
        }
    }

    /**
     * Reads data from a document
     *
     * @param string $file
     * @return mixed
     * @throws \Exception
     */
    public function read(string $file) : ?object
    {
        if (!$fp = @fopen($filename = $this->path . $file, 'r'))
        {
            throw new \Exception('Unable to open storage: ' . $file);
        }

        $attempt = 0;
        while (!flock($fp, LOCK_SH | LOCK_NB))
        {
            if (++$attempt > READ_ATTEMPTS)
            {
                fclose($fp);
                throw new \Exception('Read lock timeout exceeded: ' . intval(READ_ATTEMPTS * READ_LOCK_WAIT / 1000) . ' ms');
            }
            usleep(READ_LOCK_WAIT);
        }

        $raw = fread($fp, filesize($filename));

        flock($fp, LOCK_UN);
        $data = $raw ? json_decode($raw) : null;
        fclose($fp);

        return $data ?? null;
    }

    /**
     * Writes data to a document
     *
     * @param string $file
     * @param mixed $data
     * @throws \Exception
     */
    public function write(string $file, object $data) : void
    {
        if (!$fp = fopen($this->path . $file, 'w'))
        {
            throw new \Exception('Unable to open storage: ' . $file);
        }

        $attempt = 0;
        $raw = json_encode($data, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_NUMERIC_CHECK);

        while (!flock($fp, LOCK_EX | LOCK_NB))
        {
            if (++$attempt > WRITE_ATTEMPTS)
            {
                fclose($fp);
                throw new \Exception('Write lock timeout exceeded: ' . intval(WRITE_ATTEMPTS * WRITE_LOCK_WAIT / 1000) . ' ms');
            }
            usleep(WRITE_LOCK_WAIT);
        }

        fwrite($fp, $raw, strlen($raw));
        flock($fp, LOCK_UN);
        fclose($fp);
    }
}

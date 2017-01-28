# DoruDB
A simple file-based database. Stores data in JSON format. Supports basic CRUD operations and concurrency.
The name comes from a Proto-Indo-European word 'doru' which meant 'a tree'. This database is one tool in a
bigger collection with this name.

# Usage
#### Connecting to the database
```php
use \DoruDB\Database;

// if no arguments are passed to constructor the database is stored in 'db' folder
$db = new Database('path/to/db');
```

#### Inserting a record
```php
$user = $db->create('user', ['name' => 'That Guy']);

// this will insert a document with a unique id and nothing else
$user = $db->create('user');

// this will also insert a record, as update() works as 'upsert' if ID is given
$user = $db->update('user', ['id' => 1337, 'name' => 'This Guy']);
```

#### Finding a record
```php
// this is the fastest way to fetch a document, but you need to have an ID
$user = $db->findById('user', 1337);

// 'find' returns one record
$users = $db->find('user', ['filter' => ['name' => 'This Guy']]);

// 'findAll' returns all. NB: filter can use user-defined functions
$users = $db->find('user', ['filter' => ['name' => function ($x)
{ 
    return strpos($x, 'Guy') !== false;
} ]]);
```

#### Limit/offset
```php
$users = $db->findAll('user', ['offset' => 1, 'limit' => 1]);
```

Using limit/offset with filters is slower than just limit/offset, as the database has to read all documents first
to apply limit and offset.


#### Sorting
Only sorting by ID is supported for now. Use 'invert' option to use descending sorting.

```php
$users = $db->findAll('user', ['invert' => true]);
```

#### Removing a record
```php
// deletes one document
$db->delete('user', 1337);

// deletes all documents
$db->truncate('user');
```

# License
This project is licensed under the terms of [**MIT**](https://github.com/punarinta/doru-db/blob/master/LICENSE) license.

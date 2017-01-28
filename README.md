# DoruDB
A simple file-based database. Stores data in JSON format. Supports basic CRUD operations and concurrency.

# Sample usage
```
use \DoruDB\Driver;

$db = new Database;

// insert a record
$user = $db->create('user', ['name' => 'That Guy']);

// update a record
$user->name = 'This Guy';
$user->birthday = '2017.01.28';
$user = $db->update('user', $user);

// read a record
$user = $db->findById('user', $user->id);
print_r($user);

// read all records
print_r($db->findAll('user'));

// delete a record
$db->delete('user', $user);
```

# License
This project is licensed under the terms of [**MIT**](https://github.com/punarinta/doru-db/blob/master/LICENSE) license.

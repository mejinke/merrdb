# merrdb
一个简单、快速的Mysql数据库操作类库。

### 安装

```
composer require mejinke/merrdb
```

### 快速入门

```php
require __DIR__.'/../vendor/autoload.php';

//创建一个连接
$conn = new \Merrdb\Connection([
    'host' => 'localhost',
    'database' => 'test',
    'username' => 'root',
    'password' => '123456'
]);

//实例化Merrdb，同时传递连接
$mdb = new \Merrdb\Merrdb([$conn]);

//设置接下来要操作的表以及表的主键（如果不使用get方法可以不设置主键）
$mdb->table('user')->id('user_id');

//开启debug ，将会输出SQL
$mdb->debug();

//查询全表
$rows = $mdb->select();
print_r($rows);


$mdb->debug();
//查询主键值为1的数据
$row = $mdb->get(1);
```

### 条件表达式

通过在条件字段尾部添加英文逗号`,`来注明要使用的表达式，默认为`等于`。
包括 `=`、`!`、`>`、`>=` 、`<`、`<=`、`<>`、`><`。

```php
//等于 > SELECT * FROM user WHERE (`name` = '张三')
$mdb->select([
    "name" => "张三"
]);

//不等于 > SELECT * FROM user WHERE (`name` != '张三')
$mdb->select([
    "name,!" => "张三"
]);
//大于 > SELECT * FROM `user` WHERE (`user_id` > '1')
$mdb->select([
    "user_id,>" => 1
]);

//IN > SELECT * FROM user WHERE (`name` IN('张三', '李四', '王五'))
$mdb->select([
    "name" => ["张三", "李四", "王五"]
]);

//NOT IN > SELECT * FROM user WHERE (`name` NOT IN('张三', '李四', '王五'))
$mdb->select([
    "name,!" => ["张三", "李四", "王五"]
]);

//BETWEEN > SELECT * FROM user WHERE (`user_id` BETWEEN '1' AND '10' )
$mdb->select([
    "user_id,<>" => [1, 10] 
]);
//SELECT * FROM `user` WHERE (`user_id` BETWEEN '1' AND '1' )
$mdb->select([
    "user_id,<>" => 10 
]);

//NOT BETWEEN > SELECT * FROM user WHERE (`id` NOT BETWEEN '1' AND '10') 
$mdb->select([
    "user_id,><" => [1, 10] 
]);
```


### 事务

```php

$mdb->action(function($mdb){
    $mdb->update(['name' => 'tian'], ['id' => 1]);
    //...
    return true;
})
//当匿名函数返回false时该函数类所有的数据库操作将会回滚，如果没有返回或返回不为false时则全部提交
```
### 手动切换连接
Merrdb允许用户手动切换到不同的连接源，通过`setDispatchConnDelegate` 方法来设置，该方法需要一个`Closure` 做为参数，该参数返回一个连接的id名
```php
//创建一个连接
$conn = new \Merrdb\Connection([
    'id' => 's1', //该连接的id名，唯一
    'host' => 'localhost',
    'database' => 'test',
    'username' => 'root',
    'password' => '123456'
]);
//创建另一个连接
$conn2 = new \Merrdb\Connection([
    'id' => 's2', //该连接的id名，唯一
    'host' => 'localhost',
    'database' => 'test',
    'username' => 'root',
    'password' => '123456',
    'charset' => 'utf8'
]);

$mdb = new \Merrdb\Merrdb([$conn, $conn2]);
$mdb->table('user')->id('user_id');

//设置连接分配委托
$mdb->setDispatchConnDelegate(function(){
    return 's2';
});

$mdb->debug();

$row = $mdb->get(1);

print_r($row);
```
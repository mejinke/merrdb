<?php
require __DIR__.'/../vendor/autoload.php';
echo "<pre>";
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
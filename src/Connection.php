<?php
namespace Merrdb;

/**
 *
 * 数据连接驱动类
 *
 * @author mejinke
 * @package Merrdb
 */
class Connection
{
    protected $id;

    protected $workCount;

    protected $charset;

    protected $database;

    protected $host;

    protected $username;

    protected $password;

    protected $port;

    protected $prefix;

    protected $socket;

    protected $options;

    /**
     *
     * Allow tables
     *
     * @var array
     */
    protected $allows = [];

    /**
     *
     * Pdo
     *
     * @var \PDO
     */
    protected $pdo;

    public function __construct(array $options = null)
    {
        if (!empty($options))
        {
            foreach ($options as $key => $value)
            {
                $this->$key = $value;
            }
        }

        //随机分配id
        if ($this->id == '')
        {
            $this->id = uniqid();
        }
    }

    /**
     *
     * 获取id
     *
     * @return string
     */
    public function getId()
    {
        return $this->id;
    }

    public function getSource()
    {
       return $this->socket ?: $this->host;
    }

    /**
     *
     * 获取执行次数
     *
     * @return int
     */
    public function getWorkCount()
    {
        return intval($this->workCount);
    }

    /**
     *
     * 连接数据库
     *
     * @return $this
     */
    public function connect()
    {
        if ($this->pdo != null)
        {
            return $this;
        }

        $commands = ['SET SQL_MODE=ANSI_QUOTES'];

        if ($this->charset != '')
        {
            $commands[] = "SET NAMES '{$this->charset}'";
        }

        $this->workCount = 0;

        $this->pdo = new \PDO($this->getDsn(), $this->username, $this->password, $this->options);

        foreach ($commands as $cmd)
        {
            $this->pdo->exec($cmd);
        }

        return $this;
    }

    /**
     *
     * 获取DSN
     *
     * @return string
     */
    private function getDsn()
    {
        if ($this->socket != null)
        {
            return 'mysql:unix_socket='.$this->socket.';dbname='.$this->database;
        }

        return 'mysql:host='.$this->host.';port='.($this->port ?: 3306).';dbname='.$this->database;
    }

    /**
     *
     * 是否允许指定表操作
     *
     * @param $table
     * @return bool
     */
    public function isAllow($table)
    {
        return in_array($table, $this->allows) || empty($this->allows);
    }

    /**
     *
     * 执行SQL
     *
     * @param $query
     * @return \PDOStatement
     */
    public function query($query)
    {
        return $this->pdo->query($query, 2);
    }

    /**
     *
     * 执行SQL
     *
     * @param $query
     * @return int
     */
    public function exec($query)
    {
        return $this->pdo->exec($query);
    }

    /**
     *
     * 执行事务
     *
     * @param \Closure $action
     * @return bool
     */
    public function action(\Closure $action)
    {

        $this->pdo->beginTransaction();

        $r = $action($this);
    }

    public function error()
    {
        return $this->pdo->errorInfo();
    }
}
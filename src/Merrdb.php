<?php
namespace Merrdb;

class Merrdb
{
    const QUERY_TYPE_SELECT = 1;
    const QUERY_TYPE_INSERT = 2;
    const QUERY_TYPE_UPDATE = 3;
    const QUERY_TYPE_DELETE = 4;
    /**
     * Connection
     * @var \Merrdb\Connection[]
     */
    protected $connections;

    /**
     * 最后一次使用的连接id
     * @var string
     */
    protected $lastConnectId;

    /**
     * 当前操作的Table
     * @var string
     */
    protected $table;

    /**
     * 主键名
     * @var string
     */
    protected $id;

    /**
     * SQLs
     * @var array
     */
    protected $logs;

    /**
     * Debug
     * @var bool
     */
    protected $debug;

    /**
     * 连接分配委托
     * @var \Closure
     */
    private $dispatchConnDelegate;

    /**
     * Merrdb constructor.
     * @param \Merrdb\Connection[] $connections
     */
    public function __construct(array $connections = [])
    {
        $this->connections = (array)$connections;

        if (empty($connections) == false)
        {
            foreach ($connections as $conn)
            {
                $this->connections[$conn->getId()] = $conn;
            }
        }
    }

    /**
     * Getter|Setter
     * @param null $table
     * @return Merrdb|string|null
     */
    public function table($table = null)
    {
        if ($table == null)
        {
            return $this->table;
        }
        $this->table = (string)$table;

        return $this;
    }

    /**
     * Getter|Setter
     * @param $id
     * @return Merrdb|string|null
     */
    public function id($id = null)
    {
        if ($id == null)
        {
            return $this->id;
        }
        $this->id = (string)$id;

        return $this;
    }

    /**
     * 设置连接分配委托
     * @param \Closure $delegate
     * @return $this
     */
    public function setDispatchConnDelegate(\Closure $delegate)
    {
        $this->dispatchConnDelegate = $delegate;

        return $this;
    }

    /**
     * 开启Debug
     * @return Merrdb
     */
    public function debug()
    {
        $this->debug = true;

        return $this;
    }

    /**
     * 执行SQL
     * @param $query
     * @return \PDOStatement
     */
    public function query($query, $fetchAll = true)
    {
        $conn = $this->dispatchConnection()->connect();

        $this->saveQueryLog($query);

        return $this->queryResultFormat($conn->query($query), $fetchAll);
    }

    /**
     * 格式化Query结果
     * @param \PDOStatement $result
     * @param  bool $isetchAll
     * @return bool
     */
    protected function queryResultFormat($result, $fetchAll = true)
    {
        if ($result == false)
        {
            return false;
        }

        return $fetchAll ? $result->fetchAll() : $result->fetch();
    }

    /**
     * 执行SQL
     * @param $query
     * @return int
     */
    public function exec($query)
    {
        $conn = $this->dispatchConnection()->connect();

        $this->saveQueryLog($query);

        return $conn->exec($query);
    }

    /**
     * 获取主键记录
     * @param $id
     * @param string $column
     * @return \PDOStatement
     */
    public function get($id, $colums = '*')
    {
        return $row = $this->query($this->getNormalSQL($colums, [$this->id => $id]), false);
    }

    /**
     * 查询一行

     */
    public function fetch(array $conditions, $colums = '*')
    {
        return $row = $this->query($this->getNormalSQL($colums, $conditions), false);
    }

    /**
     * 查询多行

     */
    public function select(array $conditions = [], $colums = '*')
    {
        return $row = $this->query($this->getNormalSQL($colums, $conditions));
    }

    /**
     * 插入数据
     * @param array $data
     * @return int
     */
    public function insert(array $data)
    {
        return $this->exec($this->getNormalSQL($data, null, Merrdb::QUERY_TYPE_INSERT));
    }

    /**
     * 更新数据
     * @param array $data
     * @param array $conditions
     * @return int
     */
    public function update(array $data, array $conditions)
    {
        return $this->exec($this->getNormalSQL($data, $conditions, Merrdb::QUERY_TYPE_UPDATE));
    }

    /**
     * 删除数据
     * @param array $conditions
     * @return int
     */
    public function delete(array $conditions)
    {
        return $this->exec($this->getNormalSQL(null, $conditions, Merrdb::QUERY_TYPE_DELETE));
    }

    /**
     * 执行事务
     * @param \Closure $action
     * @return bool
     */
    public function action(\Closure $action)
    {
        $conn = $this->dispatchConnection()->connect();

        $conn->actionBegin();

        $result = $action($this);

        if ($result === false)
        {
            $conn->actionRollback();

            return false;
        }

        $conn->actionCommit();

        return $result;
    }

    /**
     * 获取完整的SQL
     * @param $colums
     * @param array $conditions
     * @param int $type
     * @return string
     */
    protected function getNormalSQL($colums, array $conditions, $type = Merrdb::QUERY_TYPE_SELECT)
    {
        switch ($type)
        {
            case Merrdb::QUERY_TYPE_SELECT:
                return "SELECT {$colums} FROM `{$this->table}` WHERE {$this->conditionParse($conditions)}";
            case Merrdb::QUERY_TYPE_INSERT:
                return "INSERT INTO `{$this->table}` SET {$this->getInsertUpdateKvData($colums)}";
            case Merrdb::QUERY_TYPE_UPDATE:
                return "UPDATE `{$this->table}` SET {$this->getInsertUpdateKvData($colums)} WHERE {$this->conditionParse($conditions)}";
            case Merrdb::QUERY_TYPE_DELETE:
                return "DELETE FROM `{$this->table}` WHERE {$this->conditionParse($conditions)}";
        }

        return '';
    }

    protected function getInsertUpdateKvData(array $colums)
    {
        $n = [];
        foreach ($colums as $colum => $val)
        {
            $n[] = "`{$colum}` = '{$val}'";
        }

        return implode(',', $n);
    }

    /**
     * 条件解析
     * @param array $conditions
     * @return string
     */
    public function conditionParse(array $conditions)
    {

        if (empty($conditions))
        {
            return '1';
        }

        $conditionReal = [];

        foreach ($conditions as $expression => $condition)
        {
            switch (trim($expression))
            {
                case 'AND':
                case 'OR':
                    foreach ($condition as $column => $value)
                    {
                        $conditionReal[trim($expression)][$column] = $value;
                    }
                    break;
                case 'ORDER':
                case 'LIMIT':
                default:
                    $conditionReal['AND'][$expression] = $condition;
            }
        }

        $vals = [];

        foreach ($conditionReal as $key => $conds)
        {
            foreach ($conds as $expression => $value)
            {
                $vals[$key][] = $this->parseExpression($expression, $value);
            }
        }

        $query = '';
        $i = 0;
        foreach ($vals as $key => $s)
        {
            $i++;
            $format = '(%s)';
            if (count($vals) > $i)
            {
                $format = " (%s) AND ";
            }
            $query .= sprintf($format, implode(" {$key} ", $s));
        }

        return $query;
    }

    /**
     * 表达式解析
     * @param $expression
     * @param $values
     * @return string
     * @throws \Exception
     */
    public function parseExpression($expression, $values)
    {
        $split = explode(',', $expression);

        if (count($split) == 1)
        {
            $split[1] = '=';
        }

        $ct = '';

        switch ($split[1])
        {
            case '=':
                $ct = '=';
                if (is_array($values))
                {
                    $ct = "IN('%s')";
                }
                break;
            case '>':
            case '>=':
            case '<':
            case '<=':
                $ct = $split[1];
                break;
            case '!':
                $ct = '!=';
                if (is_array($values))
                {
                    $ct = "NOT IN('%s')";
                }
                break;
            case '<>':
                $ct = "BETWEEN '%s' AND '%s'";
                break;
            case '><':
                $ct = "NOT BETWEEN '%s' AND '%s'";
                break;

        }

        if ($ct == '')
        {
            throw new \Exception("'{$expression}' cannot parse");
        }

        $str = "`{$split[0]}` {$ct} ";

        if (is_array($values))
        {
            if (in_array($split[1], ['<>', '><']))
            {
                $str = sprintf($str, $values[0], isset($values[1]) ? $values[1] : $values[0]);
            }

            if (in_array($split[1], ['=', '!']))
            {
                $str = sprintf($str, implode("','", $values));
            }
        }
        else
        {
            if (in_array($split[1], ['<>', '><']))
            {
                $str = sprintf($str, $values, $values);
            }

            elseif (in_array($split[1], ['=', '!']))
            {
                $str .= "'{$values}'";
            }
            else
            {
                $str .= "'{$values}'";
            }
        }

        return $str;
    }

    /**
     * 分配连接
     * @return Connection|null
     * @throws \Exception
     */
    private function dispatchConnection()
    {
        $id = null;

        //优先使用用户委托的连接
        if (is_callable($this->dispatchConnDelegate))
        {
            $id = call_user_func($this->dispatchConnDelegate);
        }

        if ($id != null)
        {
            if (isset($this->connections[$id]))
            {
                $this->lastConnectId = $id;

                return $this->connections[$id];
            }
            throw new \Exception('Connection "' . $id . '" not found');
        }

        $c = null;

        foreach ($this->connections as $conn)
        {
            //是否允许该表操作
            if ($conn->isAllow($this->table) == false)
            {
                continue;
            }

            if ($c == null || $conn->getWorkCount() < $c->getWorkCount())
            {
                $c = $conn;
            }
        }

        if ($c == null)
        {
            throw new \Exception('No connection available');
        }

        $this->lastConnectId = $c->getId();

        return $c;
    }

    /**
     * 保存Query日志
     * @param $query
     */
    protected function saveQueryLog($query)
    {
        $query = "Connection({$this->lastConnectId}): {$query}";

        if ($this->debug == true)
        {
            echo $query . "\n";
            $this->debug = false;
        }

        $this->logs[] = $query;
    }

    /**
     * 获取最后一条SQL
     * @return string
     */
    public function getLastLog()
    {
        return end($this->logs);
    }

    /**
     * 获取所有的SQL
     * @return array
     */
    public function getLogs()
    {
        return $this->logs;
    }

}
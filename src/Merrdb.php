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
     *
     * 错误信息
     *
     * @var
     */
    private $error;

    /**
     * Merrdb constructor.
     *
     * @param \Merrdb\Connection[] $connections
     */
    public function __construct(array $connections = [])
    {
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
     *
     * @param null $table
     *
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
     *
     * @param $id
     *
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
     *
     * @param \Closure $delegate
     *
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
     *
     * @param $query
     * @param bool $fetchAll
     *
     * @throws \Exception
     * @return bool
     */
    public function query($query, $fetchAll = true)
    {
        $conn = $this->dispatchConnection()->connect();
        $this->saveQueryLog($query);
        $ret = $conn->query($query);
        $this->error = $conn->error()[2];

        if ($this->error != '')
        {
            throw new \Exception($this->error. ' SQL:'.$query);
        }

        return $this->queryResultFormat($ret, $fetchAll);
    }

    /**
     * 格式化Query结果
     *
     * @param \PDOStatement $result
     * @param  bool $fetchAll
     *
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
     *
     * @param $query
     *
     * @throws \Exception
     * @return int
     */
    public function exec($query)
    {
        $conn = $this->dispatchConnection()->connect();
        $this->saveQueryLog($query);
        $ret = $conn->exec($query);
        $this->error = $conn->error()[2];

        if ($this->error != '')
        {
            throw new \Exception($this->error. ' SQL:'.$query);
        }

        return $ret;
    }

    /**
     * 获取主键记录
     *
     * @param $id
     * @param string $columns
     *
     * @return array|false
     */
    public function get($id, $columns = '*')
    {
        return $this->query($this->getNormalSQL($columns, [$this->id => $id]), false);
    }

    /**
     * 获取一条记录
     *
     * @param array $conditions
     * @param string $columns
     *
     * @return array|false
     */
    public function fetch(array $conditions, $columns = '*')
    {
        return $this->query($this->getNormalSQL($columns, $conditions), false);
    }

    /**
     * 查询多行
     *
     * @param array $conditions
     * @param string $columns
     *
     * @return array|false
     */
    public function select(array $conditions = [], $columns = '*')
    {
        return $row = $this->query($this->getNormalSQL($columns, $conditions));
    }

    /**
     * 是否存在数据
     *
     * @param array $conditions
     *
     * @return bool
     */
    public function has(array $conditions)
    {
        return $this->count($conditions) > 0;
    }

    /**
     * 查询总行数
     *
     * @param array $conditions
     *
     * @return int|mixed
     */
    public function count(array $conditions)
    {
        $row = false;

        if (isset($conditions['GROUP']))
        {
            $row = $this->query(sprintf("SELECT COUNT(*) AS RowsNum FROM (%s) as nt", $this->getNormalSQL('*', $conditions)), false);
        }
        else
        {
            $row = $this->fetch($conditions, 'COUNT(*) AS RowsNum');
        }

        return $row == false ? 0 : intval($row['RowsNum']);
    }

    /**
     * 查询指定字段的总和
     *
     * @param array $conditions
     *
     * @return number
     */
    public function sum(array $conditions, $column)
    {
        $row = $this->fetch($conditions, 'SUM('.$column.') AS SumValue');
        return $row == false ? 0 : $row['SumValue'];
    }

    /**
     * 插入数据
     *
     * @param array $data
     *
     * @return int
     */
    public function insert(array $data)
    {
        $r = $this->exec($this->getNormalSQL($data, null, Merrdb::QUERY_TYPE_INSERT));
        if ($r == 0)
        {
            return $r;
        }

        return $this->dispatchConnection()->connect()->lastInsertId();
    }

    /**
     * 更新数据
     *
     * @param array $data
     * @param array $conditions
     *
     * @return int
     */
    public function update(array $data, array $conditions)
    {
        return $this->exec($this->getNormalSQL($data, $conditions, Merrdb::QUERY_TYPE_UPDATE));
    }

    /**
     * 删除数据
     *
     * @param array $conditions
     *
     * @return int
     */
    public function delete(array $conditions)
    {
        return $this->exec($this->getNormalSQL(null, $conditions, Merrdb::QUERY_TYPE_DELETE));
    }

    /**
     * 执行事务
     *
     * @param \Closure $action
     *
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
     *
     * @param $columns
     * @param array $conditions
     * @param int $type
     *
     * @return string
     */
    protected function getNormalSQL($columns, array $conditions = null, $type = Merrdb::QUERY_TYPE_SELECT)
    {
        switch ($type)
        {
            case Merrdb::QUERY_TYPE_SELECT:
                return "SELECT {$columns} FROM `{$this->table}` {$this->parseCondition($conditions)}";
            case Merrdb::QUERY_TYPE_INSERT:
                return "INSERT INTO `{$this->table}` SET {$this->getInsertUpdateKvData($columns)}";
            case Merrdb::QUERY_TYPE_UPDATE:
                return "UPDATE `{$this->table}` SET {$this->getInsertUpdateKvData($columns)} {$this->parseCondition($conditions)}";
            case Merrdb::QUERY_TYPE_DELETE:
                return "DELETE FROM `{$this->table}` {$this->parseCondition($conditions)}";
        }

        return '';
    }

    /**
     * 获取插入和更新的SQL格式内容
     *
     * @param array $columns
     *
     * @return string
     */
    protected function getInsertUpdateKvData(array $columns)
    {
        $n = [];
        foreach ($columns as $column => $val)
        {
            if (is_array($val))
            {
                $val = implode(',', $val);
            }
            $n[] = "{$this->quoteColumn($column)} = {$this->quote($val)}";
        }

        return implode(',', $n);
    }

    /**
     * Quote
     *
     * @param $string
     *
     * @return string
     */
    protected function quote($string)
    {
        return $this->dispatchConnection()->connect()->quote($string);
    }

    /**
     * Quote column
     *
     * @param $string
     *
     * @return string
     */
    protected function quoteColumn($string)
    {
        return '`' . str_replace(['"', "'"], '', $string) . '`';
    }

    /**
     * Quote array
     *
     * @param array $values
     *
     * @return array
     */
    protected function quoteArray(array $values)
    {
        $n = [];
        foreach ($values as $v)
        {
            $n[] = $this->quote($v);
        }

        return $n;
    }

    /**
     * 条件解析
     *
     * @param array $conditions
     *
     * @return string
     */
    public function parseCondition(array $conditions)
    {
        if (empty($conditions))
        {
            return '';
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
                    $conditionReal['ORDER'] = $condition;
                    break;
                case 'GROUP':
                    $conditionReal['GROUP'] = $condition;
                    break;
                case 'LIMIT':
                    $conditionReal['LIMIT'] = is_array($condition) ? $condition : [intval($condition), intval($condition)];
                    break;
                default:
                    $parts = explode(',', $expression);
                    if (in_array(count($parts), [2, 3]) && (in_array($parts[0], ['AND','OR'])))
                    {
                        foreach ($condition as $column => $value)
                        {
                            $conditionReal[trim($expression)][$column] = $value;
                        }
                    }
                    else
                    {
                        $conditionReal['AND'][$expression] = $condition;
                    }
            }
        }

        $ws = ['AND' => [], 'AND,OR' => [], 'OR' => [], 'OR,AND' => [], 'ORDER' => [], 'GROUP' => [], 'LIMIT' => []];

        foreach ($conditionReal as $key => $conds)
        {
            switch ($key)
            {
                case 'AND':
                case 'AND,OR':
                case 'OR':
                case 'OR,AND':
                    foreach ($conds as $expression => $value)
                    {
                        $ws[$key][] = $this->parseExpression($expression, $value);
                    }
                    break;
                case 'ORDER':
                    if (is_array($conds))
                    {
                        foreach ($conds as $column => $v)
                        {
                            $ws[$key][] = "{$this->quoteColumn($column)} $v";
                        }
                    }
                    else
                    {
                        $ws[$key][] = "{$conds}";
                    }
                    break;
                case 'GROUP':
                    if (is_array($conds))
                    {
                        foreach ($conds as $column)
                        {
                            $ws[$key][] = "{$this->quoteColumn($column)}";
                        }
                    }
                    else
                    {
                        $ws[$key][] = "{$conds}";
                    }
                    break;
                case 'LIMIT':
                    $ws[$key] = $conds;
                    break;
                default:
                    //兼容 OR,AND,2 多个OR 或AND的查询
                    $parts = explode(',', $key);
                    if (count($parts) == 3 && (in_array($parts[0], ['AND','OR'])))
                    {
                        foreach ($conds as $expression => $value)
                        {
                            $ws[$key][] = $this->parseExpression($expression, $value);
                        }
                    }
            }
        }

        $query = '';

        //是否存在条件
        $existsCondition = false;
        $existsOrderby = false;
        foreach ($ws as $key => $stack)
        {
            if (empty($stack))
            {
                continue;
            }

            $w = '';

            switch ($key)
            {
                case 'AND':
                case 'OR':
                    $existsCondition = true;
                    $w = '(' . implode(" {$key} ", $stack) . ')';
                    if ($query != '')
                    {
                        $w = ' AND ' . $w;
                    }
                    break;
                case 'AND,OR':
                case 'OR,AND':
                    $parts = explode(',', $key);
                    $existsCondition = true;
                    $w = '(' . implode(" {$parts[1]} ", $stack) . ')';
                    if ($query != '')
                    {
                        $w = ' '.$parts[0].' ' . $w;
                    }
                    break;
                case 'ORDER':
                    $w = " ORDER BY " . implode(',', $stack);
                    $existsOrderby = true;
                    break;
                case 'GROUP':
                    $w = " GROUP BY " . implode(',', $stack);
                    break;
                case 'LIMIT':
                    $w = " LIMIT {$stack[0]},{$stack[1]}";
                    break;
                default:
                    //兼容 OR,AND,2 多个OR 或AND的查询
                    $parts = explode(',', $key);
                    if (count($parts) == 3 && in_array($parts[0], ['OR', 'AND']))
                    {
                        $existsCondition = true;
                        $w = '(' . implode(" {$parts[1]} ", $stack) . ')';
                        if ($query != '')
                        {
                            $w = ' '.$parts[0].' ' . $w;
                        }
                    }
            }

            if ($key == 'GROUP' && $existsOrderby == true)
            {
                continue;
            }

            $query .= $w;
        }

        return $existsCondition ? "WHERE {$query}" : $query;
    }

    /**
     * 表达式解析
     *
     * @param $expression
     * @param $values
     *
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

        switch (strtolower($split[1]))
        {
            case '=':
                $ct = '=';
                if (is_array($values))
                {
                    $ct = "IN(%s)";
                }
                elseif (is_null($values))
                {
                    $ct = "IS";
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
                    $ct = "NOT IN(%s)";
                }
                elseif (is_null($values))
                {
                    $ct = "IS NOT";
                }
                break;
            case '<>':
                $ct = "BETWEEN %s AND %s";
                break;
            case '><':
                $ct = "NOT BETWEEN %s AND %s";
                break;
            case '~':
                $ct = "LIKE ";
                break;
            case 'fin':
                $ct = " FIND_IN_SET(%s, %s)";
                break;
        }

        if ($ct == '')
        {
            throw new \Exception("'{$expression}' cannot parse");
        }

        $str = "{$this->quoteColumn($split[0])} {$ct} ";
        if (strtolower($split[1]) == 'fin')
        {
            $str = "{$ct} ";
        }

        if (is_array($values))
        {
            $valuesBk = $values;
            $values = $this->quoteArray($values);

            if (in_array($split[1], ['<>', '><']))
            {
                $str = sprintf($str, $values[0], isset($values[1]) ? $values[1] : $values[0]);
            }
            elseif(strtolower($split[1]) == 'fin')
            {
                $str = sprintf($str, $this->quote(implode(",", $valuesBk)), $split[0]);
            }
            elseif (in_array($split[1], ['=', '!']))
            {
                $str = sprintf($str, implode(",", $values));
            }
        }
        else
        {
            if (in_array($split[1], ['<>', '><']))
            {
                $str = sprintf($str, $this->quote($values), $this->quote($values));
            }

            elseif (in_array($split[1], ['=', '!']))
            {
                $str .= "{$this->quote($values)}";
            }
            elseif ($split[1] == '~')
            {
                $values = "%{$values}%";
                $str .= "{$this->quote($values)}";
            }
            elseif(strtolower($split[1]) == 'fin')
            {
                $str = sprintf($str, $this->quote($values), $split[0]);
            }
            else
            {
                $str .= "{$this->quote($values)}";
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
     *
     * 获取错误
     *
     * @return mixed
     */
    public function getError()
    {
        return $this->error;
    }

    /**
     * 保存Query日志
     *
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
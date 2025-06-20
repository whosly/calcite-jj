# avacita

使用 avatica 1.26.0, calcite 1.35.0 实现的jdbc驱动的连接和查询，并 在server端（即AvacitaConnectQueryMaskServer等server实现中），实现查询sql的改写将指定字段进行改写，实现脱敏处理。

## client - 客户端

## server-connect - 服务端： 连接
## server-connect-syntax - 服务端： SQL 识别

## server-query-mask - 服务端： 脱敏实现

## server-query-mask-rewrite - 服务端： 脱敏实现-SQL改写

* 自动识别主查询、子查询、表别名、字段别名
* 支持多表、子查询、复杂 SQL
* 对所有需要脱敏的列自动包裹脱敏函数
* 递归支持：
  嵌套子查询（SELECT ... FROM (SELECT ...) ...）
  CTE（WITH ... AS ...）
  UNION/INTERSECT/EXCEPT 等集合操作
  自动处理别名、主查询、子查询、复杂表达式


## server-all-enc - 服务端： SQL加解密实现

    

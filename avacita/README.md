# avacita

使用 avatica 1.26.0, calcite 1.39.0 实现的jdbc驱动的连接和查询，并 在server端（即AvacitaConnectQueryMaskServer等server实现中），实现查询sql的改写将指定字段进行改写，实现脱敏处理。

## client - 客户端
avatica 的客户端实现
* AvacitaClient
* AvacitaPrepareClient

## server-connect - 服务端： 连接
avatica 发送 sql , server-connect 端接收 sql

## server-connect-syntax - 服务端： SQL 识别
avatica 发送 sql , server-connect-syntax 端接收 sql， 将执行sql 自动添加 sql  hint 并执行。

## server-query-mask - 服务端： 脱敏实现
avatica 发送 sql , server 端根据脱敏字段（来自配置文件 $project/resources/mask/masking_rules.csv），
通过 desensitizeFrame 的方式， 进行脱敏处理。

## server-query-mask-rewrite - 服务端： 脱敏实现-SQL改写
avatica 发送 sql , server 端根据脱敏字段（来自配置文件 $project/resources/mask/masking_rules.csv）， 通过改写执行SQL的方式， 进行脱敏处理。

更新 masking_rules.csv 规则：
* mask_right,3,TRUE：保留右3位
* mask_middle,3,4,TRUE：保留左3位、右4位
* mask_left,4,TRUE：保留左4位
* mask_full,,TRUE：全掩码，无需参数
* hash,,TRUE：哈希，无需参数
* round,100,TRUE：四舍五入到100
* keep,,TRUE：不脱敏
* regex,\\d{3},***,TRUE：正则脱敏（如有）

* 自动识别主查询、子查询、表别名、字段别名
* 支持多表、子查询、复杂 SQL
* 对所有需要脱敏的列自动包裹脱敏函数
* 递归支持：
  嵌套子查询（SELECT ... FROM (SELECT ...) ...）
  CTE（WITH ... AS ...）
  UNION/INTERSECT/EXCEPT 等集合操作
  自动处理别名、主查询、子查询、复杂表达式
* SELECT *、多表、别名、子查询、CTE、UNION、窗口函数等复杂SQL 
* 多种脱敏策略（如全掩码、左掩码、右掩码、部分掩码、哈希、正则等） 
* 非标准 SQL 语句直接透传，不做解析和重写。 只对标准 SQL 语句做脱敏重写。

## server-query-mask-rewrite-rule - 服务端： 脱敏实现-rule SQL改写
avatica 发送 sql , server 端根据脱敏字段（来自配置文件 $project/resources/mask/masking_rules.csv）， 
通过rule 改写执行SQL的方式， 进行脱敏处理。

## server-all-enc - 服务端： SQL加解密实现

    

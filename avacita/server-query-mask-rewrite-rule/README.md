# SQL查询脱敏服务

基于Apache Calcite 1.39.0 和Avatica 1.26.0 实现的SQL查询脱敏服务，支持多种脱敏策略和动态配置。

## 功能特性

- ✅ **SQL重写脱敏**: 在SQL执行前重写查询语句，对敏感列应用脱敏函数
- ✅ **结果集脱敏**: 对查询结果进行二次脱敏处理
- ✅ **多种脱敏策略**: 支持左掩码、右掩码、中间掩码、完全掩码、哈希、四舍五入、正则替换等
- ✅ **动态配置**: 支持CSV配置文件热加载，无需重启服务
- ✅ **复杂SQL支持**: 支持SELECT *、子查询、JOIN、UNION、CTE等复杂SQL
- ✅ **表别名支持**: 自动识别和处理表别名
- ✅ **MySQL方言**: 使用MySQL语法和函数

## 脱敏策略

| 策略类型 | 描述 | 参数 | 示例 |
|---------|------|------|------|
| `mask_full` | 完全脱敏 | 无 | `******` |
| `mask_left` | 左掩码 | 保留字符数 | `abc******` |
| `mask_right` | 右掩码 | 保留字符数 | `******xyz` |
| `mask_middle` | 中间掩码 | 左保留数,右保留数 | `abc******xyz` |
| `hash` | 哈希脱敏 | 无 | `5d41402abc4b2a76b9719d911017c592` |
| `round` | 四舍五入 | 舍入基数 | `1000` |
| `regex` | 正则替换 | 正则表达式 | `******` |
| `keep` | 保持原值 | 无 | 原始值 |

## 配置文件

配置文件位置: `src/main/resources/mask/masking_rules.csv`

```csv
schema,table,column,rule_type,rule_params...,enabled
demo,t_emp,id,keep,,TRUE
demo,t_emp,name,mask_right,3,TRUE
demo,t_emp,tel,mask_middle,3,4,TRUE
demo,t_emp,cert_no,mask_full,,TRUE
demo,t_emp,email,mask_left,4,TRUE
public,orders,order_id,keep,,TRUE
public,orders,amount,round,100,TRUE
public,orders,customer_id,hash,,TRUE
```

### 配置说明

- `schema`: 数据库schema名称
- `table`: 表名
- `column`: 列名
- `rule_type`: 脱敏策略类型
- `rule_params`: 策略参数（多个参数用逗号分隔）
- `enabled`: 是否启用（TRUE/FALSE）

## 使用方法

### 1. 启动服务

```bash
# 编译项目
mvn clean compile

# 启动脱敏服务
mvn exec:java -Dexec.mainClass="com.whosly.avacita.server.query.mask.rewrite.rule.AvacitaConnectQueryMaskRewriteRuleServer"
```

服务将在端口5888上启动。

### 2. 客户端连接

```java
// 注册Avatica驱动
Class.forName("org.apache.calcite.avatica.remote.Driver");

Properties props = new Properties();
props.put("serialization", "protobuf");

String url = "jdbc:avatica:remote:url=http://localhost:5888";

try (Connection conn = DriverManager.getConnection(url, props)) {
    // 执行查询
    String sql = "SELECT id, name, tel, email FROM t_emp LIMIT 5";
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
        // 处理结果
        while (rs.next()) {
            // 结果已经自动脱敏
            System.out.println(rs.getString("tel")); // 显示脱敏后的电话号码
        }
    }
}
```

### 3. 测试脱敏效果

```java
// 原始数据
// id=1001, name="张三", tel="13812345678", email="zhangsan@example.com"

// 查询结果（已脱敏）
// id=1001, name="******张三", tel="138******5678", email="zhan******"
```

## 支持的SQL特性

### 基本查询
```sql
SELECT id, name, tel FROM t_emp WHERE id > 1000;
```

### SELECT * 查询
```sql
SELECT * FROM t_emp LIMIT 10;
-- 自动展开为所有列并应用脱敏
```

### 表别名
```sql
SELECT e.id, e.name, e.tel FROM t_emp e WHERE e.id < 2000;
```

### JOIN查询
```sql
SELECT e.name, d.dept_name, e.tel 
FROM t_emp e 
JOIN t_dept d ON e.dept_id = d.id;
```

### 子查询
```sql
SELECT name, tel FROM t_emp 
WHERE dept_id IN (SELECT id FROM t_dept WHERE dept_name = 'IT');
```

### 聚合查询
```sql
SELECT dept_id, COUNT(*) as emp_count, AVG(salary) as avg_salary 
FROM t_emp 
GROUP BY dept_id;
```

## 技术架构

```
客户端应用
    ↓ JDBC
Avatica客户端
    ↓ HTTP/Protobuf
Avatica服务器
    ↓ SQL重写
MaskingJdbcMeta
    ↓ 脱敏处理
MySQL数据库
```

### 核心组件

1. **MaskingJdbcMeta**: 核心脱敏处理类
   - SQL解析和重写
   - 结果集脱敏
   - 配置管理

2. **MaskingConfigMeta**: 配置管理类
   - CSV配置文件解析
   - 热加载支持
   - 规则匹配

3. **MaskingRuleConfig**: 脱敏规则类
   - 规则定义
   - 参数管理
   - 策略应用

## 性能优化

- **列缓存**: 缓存表结构信息，避免重复查询元数据
- **规则缓存**: 内存中缓存脱敏规则，提高匹配速度
- **SQL重写**: 在数据库层面应用脱敏，减少数据传输
- **批量处理**: 支持大批量数据的脱敏处理

## 监控和日志

服务提供详细的日志输出：

```
INFO  - 原始SQL: SELECT id, name, tel FROM t_emp WHERE id > 1000
INFO  - 重写后SQL: SELECT id, name, CONCAT('******', RIGHT(tel, 4)) FROM t_emp WHERE id > 1000
INFO  - 脱敏处理完成，处理了 5 行数据
```

## 故障排除

### 常见问题

1. **SQL解析错误**
   - 检查SQL语法是否符合MySQL标准
   - 查看日志中的具体错误信息

2. **脱敏规则不生效**
   - 检查配置文件格式是否正确
   - 确认规则中的schema、table、column名称匹配
   - 验证enabled字段是否为TRUE

3. **连接失败**
   - 确认服务是否正常启动
   - 检查端口5888是否被占用
   - 验证数据库连接配置

### 调试模式

启用DEBUG日志级别查看详细信息：

```bash
# 设置日志级别
export LOG_LEVEL=DEBUG
```

## 扩展开发

### 添加新的脱敏策略

1. 在`MaskingRuleType`枚举中添加新类型
2. 在`MaskingJdbcMeta`中实现对应的SQL表达式生成方法
3. 在`applyMaskingStrategy`方法中添加处理逻辑

### 自定义配置源

可以扩展`MaskingConfigMeta`类，支持从数据库、配置中心等加载规则。

## 许可证

本项目基于Apache License 2.0开源协议。 
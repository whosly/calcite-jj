#!/bin/bash

echo "========================================"
echo "   SQL查询脱敏服务启动脚本"
echo "========================================"
echo

echo "正在编译项目..."
mvn clean compile -q
if [ $? -ne 0 ]; then
    echo "编译失败，请检查项目配置"
    exit 1
fi

echo "编译成功！"
echo

echo "正在启动脱敏服务..."
echo "服务端口: 5888"
echo "配置文件: src/main/resources/mask/masking_rules.csv"
echo

echo "按 Ctrl+C 停止服务"
echo "========================================"

mvn exec:java -Dexec.mainClass="com.whosly.avacita.server.query.mask.rewrite.rule.AvacitaConnectQueryMaskRewriteRuleServer" 
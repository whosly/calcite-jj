@echo off
echo ========================================
echo    SQL脱敏客户端测试脚本
echo ========================================
echo.

echo 正在编译测试代码...
call mvn test-compile -q
if %errorlevel% neq 0 (
    echo 编译失败，请检查项目配置
    pause
    exit /b 1
)

echo 编译成功！
echo.

echo 正在运行脱敏客户端测试...
echo 请确保脱敏服务已在端口5888上运行
echo.

call mvn exec:java -Dexec.mainClass="com.whosly.avacita.server.query.mask.rewrite.rule.MaskingClientTest"

echo.
echo 测试完成！
pause 
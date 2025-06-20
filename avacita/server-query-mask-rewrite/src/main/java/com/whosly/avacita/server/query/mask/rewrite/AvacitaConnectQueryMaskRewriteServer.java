package com.whosly.avacita.server.query.mask.rewrite;

import java.sql.SQLException;
import java.util.Properties;

import com.whosly.avacita.server.query.mask.mysql.MaskingConfigMeta;
import com.whosly.avacita.server.query.mask.mysql.MaskingJdbcMeta;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 查询脱敏服务
 * <p>
 *
 * calcite 1.35.0, avatica 1.26.0， 使用avatica client和 avatica server, 实现rule规则， 通过配置文件(配置允许动态变更)对查询的某字段进行脱敏操作
 */
public class AvacitaConnectQueryMaskRewriteServer {
    private static final Logger LOG = LoggerFactory.getLogger(AvacitaConnectQueryMaskRewriteServer.class);

    private static final int PORT = 5888;

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        // 初始化脱敏配置（配置文件放在resources/mask/目录下）
        MaskingConfigMeta maskingConfigMeta = new MaskingConfigMeta("mask/masking_rules.csv");

        // generateSimpleParameterMetadata=true   生成简单的参数元数据
        final String DB_URL = "jdbc:mysql://localhost:13307/demo?useUnicode=true&generateSimpleParameterMetadata=true";
        final String DB_USER = "root";
        final String DB_PASSWORD = "Aa123456.";

        // 注册MySQL驱动
        Class.forName("com.mysql.cj.jdbc.Driver");

        Properties props = new Properties();
        props.setProperty("user", DB_USER);
        props.setProperty("password", DB_PASSWORD);

        // 创建带脱敏功能的Meta实例
        final JdbcMeta meta = new MaskingJdbcMeta(DB_URL, props, maskingConfigMeta);
        final LocalService service = new LocalService(meta);

        final HttpServer server = new HttpServer.Builder<>()
                .withPort(PORT)
                .withHandler(service, Driver.Serialization.PROTOBUF)
                .build();
        server.start();
        System.out.println("服务器已启动，监听端口: " + PORT);

        // 创建并启动守护线程，等待服务器关闭
        Thread serverThread = new Thread(() -> {
            try {
                server.join(); // 阻塞当前线程，直到服务器停止
            } catch (InterruptedException e) {
                System.err.println("服务器线程被中断: " + e.getMessage());
                Thread.currentThread().interrupt();
            }
        });
        serverThread.setDaemon(false); // 设置为非守护线程（默认值，但明确写出更安全）
        serverThread.start(); // 关键：启动线程

        // 添加关闭钩子，处理优雅停机
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("接收到关闭信号，正在停止服务器...");
            try {
                server.stop();
                System.out.println("服务器已成功关闭");
            } catch (Exception e) {
                System.err.println("关闭服务器时出错: " + e.getMessage());
            }
        }));
    }
}

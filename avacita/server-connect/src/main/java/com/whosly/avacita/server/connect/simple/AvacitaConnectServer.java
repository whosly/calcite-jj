package com.whosly.avacita.server.connect.simple;

import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.HttpServer;

import java.sql.SQLException;

public class AvacitaConnectServer {

    public static void main(String[] args) throws SQLException {
        String url = "jdbc:mysql://localhost:13307/demo?useUnicode=true&generateSimpleParameterMetadata=true";

        final JdbcMeta meta = new JdbcMeta(url, "root", "Aa123456.");

        // LocalService会交给Meta，Avatica中有多种Meta实现，对于当前这种情况，Avatica中提供了一个JdbcMeta，用来代理其他的数据库的。
        // 是一种比较特殊的服务端Meta实现，内部就去请求一个其他数据库，比如mysql，然后得到结果返回给客户端。
        // 服务端收到请求，交给LocalService处理
        final LocalService service = new LocalService(meta);

        final HttpServer server = new HttpServer.Builder<>()
                .withPort(5888)
                .withHandler(service, Driver.Serialization.PROTOBUF)
                .build();
        server.start();
        System.out.println("服务器已启动，监听端口: 5888");

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
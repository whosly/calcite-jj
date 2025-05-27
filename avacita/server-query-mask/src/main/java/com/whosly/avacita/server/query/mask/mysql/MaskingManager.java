package com.whosly.avacita.server.query.mask.mysql;

import com.whosly.avacita.server.query.mask.rule.MaskingRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MaskingManager {
    private static final Logger LOG = LoggerFactory.getLogger(MaskingManager.class);
    private final String configPath;
    private final Map<String, MaskingRule> maskingRules = new ConcurrentHashMap<>();
    private long lastModifiedTime = 0;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public MaskingManager(String configPath) {
        this.configPath = configPath;
        loadConfig();
        startWatching();
    }

    // 加载脱敏配置文件
    private void loadConfig() {
        File configFile = new File(configPath);
        if (!configFile.exists()) {
            LOG.warn("脱敏配置文件不存在: {}", configPath);
            return;
        }

        try {
            // 检查文件是否有更新
            long currentModifiedTime = configFile.lastModified();
            if (currentModifiedTime == lastModifiedTime) {
                return;
            }

            // 读取配置文件
            Map<String, MaskingRule> newRules = new ConcurrentHashMap<>();
            try (BufferedReader reader = new BufferedReader(new FileReader(configFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty() || line.startsWith("#")) {
                        continue;
                    }

                    // 解析规则: schema.table.column,type,params
                    String[] parts = line.split(",", 3);
                    if (parts.length < 3) {
                        LOG.warn("无效的脱敏规则: {}", line);
                        continue;
                    }

                    String columnId = parts[0].trim();
                    String ruleType = parts[1].trim();
                    String params = parts[2].trim();

                    newRules.put(columnId, new MaskingRule(columnId, ruleType, params));
                }
            }

            // 应用新规则
            maskingRules.clear();
            maskingRules.putAll(newRules);
            lastModifiedTime = currentModifiedTime;
            LOG.info("成功加载脱敏配置文件，共加载 {} 条规则", maskingRules.size());
        } catch (Exception e) {
            LOG.error("加载脱敏配置文件失败", e);
        }
    }

    // 启动配置文件监控
    private void startWatching() {
        scheduler.scheduleAtFixedRate(this::loadConfig, 5, 5, TimeUnit.SECONDS);
    }

    // 获取字段的脱敏规则
    public MaskingRule getRule(String columnId) {
        return maskingRules.get(columnId);
    }

    // 关闭资源
    public void shutdown() {
        scheduler.shutdown();
    }
}

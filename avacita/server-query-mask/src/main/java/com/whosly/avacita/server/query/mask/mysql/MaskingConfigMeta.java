package com.whosly.avacita.server.query.mask.mysql;

import com.whosly.avacita.server.query.mask.rule.MaskingRule;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 脱敏的配置
 */
public class MaskingConfigMeta {
    private static final Logger LOG = LoggerFactory.getLogger(MaskingConfigMeta.class);
    private final String configPath;
    private final Map<String, List<MaskingRule>> maskingRules = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public MaskingConfigMeta(String configPath) {
        this.configPath = configPath;

        loadConfig();
        startWatching();
    }

    // 加载脱敏配置文件
    private void loadConfig() {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(configPath);
        if (inputStream == null) {
            throw new RuntimeException("资源未找到: " + configPath);
        }

        // 清空旧规则
        maskingRules.clear();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            boolean isHeader = true;
            while ((line = reader.readLine()) != null) {
                if (isHeader) {
                    isHeader = false;
                    continue;
                }
                String[] parts = line.split(",");
                if (parts.length >= 6 && Boolean.parseBoolean(parts[5])) {
                    String schema = parts[0];
                    String table = parts[1];
                    String column = parts[2];
                    String ruleType = parts[3];
                    String[] ruleParams = Arrays.copyOfRange(parts, 4, parts.length);

                    MaskingRule rule = new MaskingRule(
                            schema, table, column, ruleType, ruleParams
                    );

                    String key = schema + "." + table;
                    // 使用 computeIfAbsent 初始化列表，但直接添加规则（不检查是否存在）
                    maskingRules.computeIfAbsent(key, k -> new ArrayList<>()).add(rule);
                }
            }

            LOG.trace("成功加载脱敏配置: {}，规则数量: {}， 规则：{}。",
                    configPath,
                    maskingRules.values().stream().mapToInt(List::size).sum(),
                    StringUtils.join(
                            maskingRules.values().stream().map(rs ->{
                                        return rs.stream().map(r ->{
                                            return r.getSchema() + "." + r.getTable() + "." + r.getColumn();
                                        }).toList();
                                    }).toList()
                                    .stream().flatMap(List::stream).toList(),
                            ",")
            );
        } catch (IOException e) {
            throw new RuntimeException("加载脱敏配置失败", e);
        }
    }

    // 根据表名和字段名查找脱敏规则
    public List<MaskingRule> getRule(String schema, String table) {
        String key = schema + "." + table;
        List<MaskingRule> tableRules = maskingRules.getOrDefault(key, Collections.emptyList());
        return tableRules;
    }

    // 根据表名和字段名查找脱敏规则
    public MaskingRule getRule(String schema, String table, String column) {
        String key = schema + "." + table;
        List<MaskingRule> tableRules = maskingRules.getOrDefault(key, Collections.emptyList());

        MaskingRule columnRole = tableRules.stream()
                .filter(r -> r.getColumn().equalsIgnoreCase(column))
                .findFirst()
                .orElse(null);
        return columnRole;
    }

    // 启动配置文件监控
    private void startWatching() {
        scheduler.scheduleAtFixedRate(this::loadConfig, 5, 5, TimeUnit.SECONDS);
    }

    // 关闭资源
    public void shutdown() {
        scheduler.shutdown();
    }
}

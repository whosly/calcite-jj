package com.whosly.avacita.server.query.mask.rewrite.rule.rules;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 脱敏的配置
 */
public class MaskingConfigMeta {
    private static final Logger LOG = LoggerFactory.getLogger(MaskingConfigMeta.class);
    private final String configPath;
    private final Map<String, List<MaskingRuleConfig>> maskingRules = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private long lastLoadTime = 0L;

    public MaskingConfigMeta(String configPath) {
        this.configPath = configPath;

        loadConfig();
        startWatching();
    }

    // A regex to parse CSV rows. Handles quoted fields.
    private static final Pattern CSV_PATTERN = Pattern.compile("(\"[^\"]*\"|[^,]*),?");

    // 加载脱敏配置文件
    private void loadConfig() {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(configPath);
        if (inputStream == null) {
            throw new RuntimeException("资源未找到: " + configPath);
        }

        // 清空旧规则
        maskingRules.clear();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            boolean isHeader = true;
            while ((line = reader.readLine()) != null) {
                if (isHeader) {
                    isHeader = false;
                    continue;
                }
                if (line.trim().isEmpty() || line.startsWith("#")) {
                    continue;
                }

                List<String> parts = new ArrayList<>();
                Matcher matcher = CSV_PATTERN.matcher(line);
                while (matcher.find()) {
                    String part = matcher.group(1);
                    if (part != null) {
                        // Remove quotes if present
                        if (part.startsWith("\"") && part.endsWith("\"")) {
                            part = part.substring(1, part.length() - 1);
                        }
                        parts.add(part.trim());
                    }
                }
                if (line.endsWith(",")) {
                    parts.add("");
                }


                if (parts.size() < 6) continue;

                String schema = parts.get(0);
                String table = parts.get(1);
                String column = parts.get(2);
                String ruleType = parts.get(3);
                String ruleParamsStr = parts.get(4);
                boolean enabled = Boolean.parseBoolean(parts.get(5).trim());

                if (!enabled) continue;

                String[] ruleParams = parseRuleParams(ruleParamsStr);

                MaskingRuleConfig rule = new MaskingRuleConfig(
                        schema, table, column, ruleType, ruleParams
                );

                String key = schema + "." + table;
                maskingRules.computeIfAbsent(key, k -> new ArrayList<>()).add(rule);
            }

            LOG.info("成功加载脱敏配置: {}, 规则数量: {}.",
                    configPath,
                    maskingRules.values().stream().mapToInt(List::size).sum()
            );
        } catch (IOException e) {
            throw new RuntimeException("加载脱敏配置失败", e);
        }
    }

    private String[] parseRuleParams(String paramsStr) {
        if (StringUtils.isBlank(paramsStr)) {
            return new String[0];
        }
        return Arrays.stream(paramsStr.split(";"))
                .map(String::trim)
                .toArray(String[]::new);
    }

    // 新增热加载检测
    private void startWatching() {
        InputStream is = getClass().getClassLoader().getResourceAsStream(configPath);
        if (is == null) {
            LOG.warn("无法找到配置文件 {} 用于监控，热加载将不会生效。", configPath);
            return;
        }

        scheduler.scheduleAtFixedRate(() -> {
            try {
                File configFile = new File(getClass().getClassLoader().getResource(configPath).toURI());
                if (configFile.exists() && configFile.lastModified() > this.lastLoadTime) {
                    LOG.info("检测到配置文件变更，重新加载脱敏规则...");
                    loadConfig();
                    this.lastLoadTime = System.currentTimeMillis();
                }
            } catch (Exception e) {
                LOG.error("配置文件监控异常", e);
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    // 按列名匹配规则
    public MaskingRuleConfig getMatchingRule(String schema, String table, String column) {
        return maskingRules.values().stream()
                .flatMap(List::stream)
                .filter(rule -> rule.match(schema, table, column))
                .findFirst()
                .orElse(null);
    }

    // 根据表名和字段名查找脱敏规则
    public List<MaskingRuleConfig> getRule(String schema, String table) {
        String key = schema + "." + table;
        return maskingRules.getOrDefault(key, Collections.emptyList());
    }

    // 根据表名和字段名查找脱敏规则
    public MaskingRuleConfig getRule(String schema, String table, String column) {
        String key = schema + "." + table;
        List<MaskingRuleConfig> tableRules = maskingRules.getOrDefault(key, Collections.emptyList());

        return tableRules.stream()
                .filter(r -> r.getColumn().equalsIgnoreCase(column))
                .findFirst()
                .orElse(null);
    }

    // 关闭资源
    public void shutdown() {
        scheduler.shutdown();
    }
}
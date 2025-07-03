package com.whosly.avacita.server.query.mask.rewrite.rule;

import com.whosly.avacita.server.query.mask.rewrite.rule.rules.MaskingConfigMeta;
import com.whosly.avacita.server.query.mask.rewrite.rule.rules.MaskingRuleConfig;
import com.whosly.avacita.server.query.mask.rewrite.rule.rules.MaskingRuleType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 脱敏规则测试类
 */
public class MaskingRewriteRuleTest {
    private static final Logger LOG = LoggerFactory.getLogger(MaskingRewriteRuleTest.class);
    
    private MaskingConfigMeta maskingConfigMeta;

    @BeforeEach
    void setUp() {
        // 使用测试配置文件
        maskingConfigMeta = new MaskingConfigMeta("mask/masking_rules.csv");
    }

    @Test
    void testLoadConfig() {
        // 测试配置加载
        MaskingRuleConfig rule = maskingConfigMeta.getMatchingRule("demo", "t_emp", "tel");
        assertNotNull(rule);
        assertEquals(MaskingRuleType.MASK_MIDDLE, rule.getRuleType());
        assertEquals(2, rule.getRuleParams().length);
        assertEquals("3", rule.getRuleParams()[0]);
        assertEquals("4", rule.getRuleParams()[1]);
    }

    @Test
    void testMaskingStrategies() {
        // 测试左掩码
        MaskingRuleConfig leftRule = maskingConfigMeta.getMatchingRule("demo", "t_emp", "email");
        assertNotNull(leftRule);
        assertEquals(MaskingRuleType.MASK_LEFT, leftRule.getRuleType());
        
        // 测试右掩码
        MaskingRuleConfig rightRule = maskingConfigMeta.getMatchingRule("demo", "t_emp", "name");
        assertNotNull(rightRule);
        assertEquals(MaskingRuleType.MASK_RIGHT, rightRule.getRuleType());
        
        // 测试完全掩码
        MaskingRuleConfig fullRule = maskingConfigMeta.getMatchingRule("demo", "t_emp", "cert_no");
        assertNotNull(fullRule);
        assertEquals(MaskingRuleType.MASK_FULL, fullRule.getRuleType());
        
        // 测试保持原值
        MaskingRuleConfig keepRule = maskingConfigMeta.getMatchingRule("demo", "t_emp", "id");
        assertNotNull(keepRule);
        assertEquals(MaskingRuleType.KEEP, keepRule.getRuleType());
    }

    @Test
    void testMultiParamRules() {
        // 测试多参数规则（中间掩码）
        MaskingRuleConfig middleRule = maskingConfigMeta.getMatchingRule("demo", "t_emp", "tel");
        assertNotNull(middleRule);
        assertEquals(MaskingRuleType.MASK_MIDDLE, middleRule.getRuleType());
        
        String[] params = middleRule.getRuleParams();
        assertEquals(2, params.length);
        assertEquals("3", params[0]); // 左边保留字符数
        assertEquals("4", params[1]); // 右边保留字符数
    }

    @Test
    void testOtherTableRules() {
        // 测试其他表的规则
        MaskingRuleConfig roundRule = maskingConfigMeta.getMatchingRule("public", "orders", "amount");
        assertNotNull(roundRule);
        assertEquals(MaskingRuleType.ROUND, roundRule.getRuleType());
        assertEquals("100", roundRule.getRuleParams()[0]);
        
        MaskingRuleConfig hashRule = maskingConfigMeta.getMatchingRule("public", "orders", "customer_id");
        assertNotNull(hashRule);
        assertEquals(MaskingRuleType.HASH, hashRule.getRuleType());
    }

    @Test
    void testNonExistentRule() {
        // 测试不存在的规则
        MaskingRuleConfig rule = maskingConfigMeta.getMatchingRule("demo", "t_emp", "non_existent_column");
        assertNull(rule);
    }

    @Test
    void testRuleMatching() {
        // 测试规则匹配
        MaskingRuleConfig rule1 = maskingConfigMeta.getMatchingRule("demo", "t_emp", "tel");
        MaskingRuleConfig rule2 = maskingConfigMeta.getMatchingRule("DEMO", "T_EMP", "TEL");
        
        // 应该匹配相同的规则（忽略大小写）
        assertNotNull(rule1);
        assertNotNull(rule2);
        assertEquals(rule1.getRuleType(), rule2.getRuleType());
    }

    @Test
    void testConfigReload() {
        // 测试配置重新加载功能
        MaskingRuleConfig originalRule = maskingConfigMeta.getMatchingRule("demo", "t_emp", "tel");
        assertNotNull(originalRule);
        
        // 配置重新加载应该在后台线程中进行
        // 这里只是验证配置对象可以正常工作
        assertNotNull(maskingConfigMeta);
    }
} 
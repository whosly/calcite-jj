package com.whosly.avacita.server.query.mask.rule;

import org.apache.commons.lang3.StringUtils;

public enum MaskingRuleType {
    /**
     * 保持不变
     */
    KEEP,

    MASK_LEFT,

    MASK_RIGHT,

    MASK_MIDDLE,

    MASK_FULL,

    /**
     * 随机打掩码
     */
    ROUND,

    /**
     * 值转为 Hash 值
     */
    HASH,

    PARTIAL,

    REGEX,


    ;

    public static MaskingRuleType getByName(String name) {
        for (MaskingRuleType type : MaskingRuleType.values()) {
            if(StringUtils.equalsIgnoreCase(type.name(), name)) {
                return type;
            }
        }

        return MaskingRuleType.KEEP;
    }
}

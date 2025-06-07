package com.whosly.avacita.server.query.mask.util;

import com.whosly.avacita.server.query.mask.rule.MaskingRuleConfig;
import com.whosly.avacita.server.query.mask.rule.MaskingRuleType;

public class ValueMaskingStrategy {

    public static Object mask(Object value, MaskingRuleConfig rule) {
        if (value == null) {
            return null;
        }
        if (rule == null) {
            return value;
        }

        MaskingRuleType ruleType = rule.getRuleType();
        String[] params = rule.getRuleParams();

        switch (ruleType) {
            case KEEP:
                return value;
            case MASK_FULL:
                return maskFullValue(value);
            case MASK_MIDDLE:
                return maskMiddleValue(value);
            case MASK_LEFT:
                return maskLeftValue(value);
            case MASK_RIGHT:
                return maskRightValue(value, params);
            case ROUND:
                long num = ((Number)value).longValue();
                long divisor = Long.parseLong(params[0]);
                return (num / divisor) * divisor;
//            case HASH/PARTIAL/REGEX
            default:
                return value;
        }
    }

    /**
     * 全掩码处理
     */
    private static Object maskFullValue(Object value) {
        return "******";
    }

    /**
     * 中间掩码处理（保留前preLength位和后postLength位，中间间隔显示）
     */
    private static Object maskMiddleValue(Object value) {
        String strValue = value != null ? value.toString() : "";

        int preLength = 3;  // 前缀保留长度
        int postLength = 2; // 后缀保留长度

        if (strValue.length() <= preLength + postLength) {
            return maskFullValue(value); // 长度不足，降级为全掩码
        }

        StringBuilder masked = new StringBuilder();
        masked.append(strValue, 0, preLength);

        int visibleCount = 0;
        for (int i = preLength; i < strValue.length() - postLength; i++) {
            if (visibleCount % 4 == 0) { // 每4个字符显示1个
                masked.append(strValue.charAt(i));
            } else {
                masked.append('*');
            }
            visibleCount++;
        }

        masked.append(strValue.substring(strValue.length() - postLength));
        return masked.toString();
    }

    /**
     * 左掩码处理（保留左侧preLength位）
     */
    private static Object maskLeftValue(Object value) {
        String strValue = value != null ? value.toString() : "";

        int preLength = 4; // 前缀保留长度

        if (strValue == null || strValue.length() <= preLength) {
            return value; // 长度不足或为空，不做掩码
        }

        StringBuilder masked = new StringBuilder(strValue.substring(0, preLength));
        for (int i = preLength; i < strValue.length(); i++) {
            masked.append('*');
        }
        return masked.toString();

//        return StringUtils.leftPad("", ((String)value).length(), '*');
    }

    /**
     * 右掩码处理（保留右侧postLength位）
     */
    private static Object maskRightValue(Object value, String[] params) {
        String strValue = value != null ? value.toString() : "";

        int postLength = 4; // 后缀保留长度

        if (strValue.length() <= postLength) {
            return value; // 长度不足，不做掩码
        }

        StringBuilder masked = new StringBuilder();
        for (int i = 0; i < strValue.length() - postLength; i++) {
            masked.append('*');
        }

        masked.append(strValue.substring(strValue.length() - postLength));
        return masked.toString();

//        int keepRight = Integer.parseInt(params[0]);
//        String strVal = value.toString();
//        return strVal.length() > keepRight ?
//                StringUtils.leftPad(strVal.substring(strVal.length() - keepRight), strVal.length(), '*')
//                : strVal;
    }
}

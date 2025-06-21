package com.whosly.avacita.core.enc;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.HashMap;
import java.util.Map;
import java.util.Base64;
import java.util.regex.Pattern;

public class ValueAnalyzer extends JFrame {
    private JTextArea inputArea;
    private JTextArea resultArea;
    private JButton analyzeButton;
    private JButton clearButton;
    private JPanel chartPanel;
    private JLabel entropyLabel;
    private JLabel lengthLabel;
    private JLabel charsetLabel;
    private JLabel base64Label;
    private JLabel hexLabel;
    private JLabel cipherLabel;

    public ValueAnalyzer() {
        setTitle("Database Value Analyzer - SM4 Encryption Detection");
        setSize(800, 600);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(new BorderLayout(10, 10));

        // 输入面板
        JPanel inputPanel = new JPanel(new BorderLayout());
        inputPanel.setBorder(BorderFactory.createTitledBorder("Input Field Value"));
        inputArea = new JTextArea(5, 50);
        inputArea.setFont(new Font("Monospaced", Font.PLAIN, 14));
        JScrollPane inputScroll = new JScrollPane(inputArea);
        inputPanel.add(inputScroll, BorderLayout.CENTER);

        // 按钮面板
        JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER, 10, 10));
        analyzeButton = new JButton("Analyze Value");
        clearButton = new JButton("Clear");
        buttonPanel.add(analyzeButton);
        buttonPanel.add(clearButton);

        // 结果面板
        JPanel resultPanel = new JPanel(new BorderLayout());
        resultPanel.setBorder(BorderFactory.createTitledBorder("Analysis Results"));
        resultArea = new JTextArea(10, 50);
        resultArea.setEditable(false);
        resultArea.setFont(new Font("Monospaced", Font.PLAIN, 14));
        resultArea.setForeground(new Color(0, 100, 0));
        JScrollPane resultScroll = new JScrollPane(resultArea);
        resultPanel.add(resultScroll, BorderLayout.CENTER);

        // 指标面板
        JPanel metricsPanel = new JPanel(new GridLayout(2, 3, 5, 5));
        metricsPanel.setBorder(BorderFactory.createTitledBorder("Key Metrics"));

        entropyLabel = createMetricLabel("Entropy: -");
        lengthLabel = createMetricLabel("Length: -");
        charsetLabel = createMetricLabel("Charset: -");
        base64Label = createMetricLabel("Base64: -");
        hexLabel = createMetricLabel("Hex: -");
        cipherLabel = createMetricLabel("Cipher: -");
        metricsPanel.add(entropyLabel);
        metricsPanel.add(lengthLabel);
        metricsPanel.add(charsetLabel);
        metricsPanel.add(base64Label);
        metricsPanel.add(hexLabel);
        metricsPanel.add(cipherLabel);

        // 图表面板
        chartPanel = new JPanel() {
            @Override
            protected void paintComponent(Graphics g) {
                super.paintComponent(g);
                drawChart(g);
            }
        };
        chartPanel.setPreferredSize(new Dimension(300, 150));
        chartPanel.setBorder(BorderFactory.createTitledBorder("Analysis Visualization"));

        // 添加组件到主窗口
        add(inputPanel, BorderLayout.NORTH);
        add(buttonPanel, BorderLayout.CENTER);
        add(resultPanel, BorderLayout.SOUTH);

        JPanel centerPanel = new JPanel(new BorderLayout());
        centerPanel.add(metricsPanel, BorderLayout.NORTH);
        centerPanel.add(chartPanel, BorderLayout.CENTER);
        add(centerPanel, BorderLayout.EAST);

        // 添加事件监听器
        analyzeButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                analyzeValue();
            }
        });

        clearButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                inputArea.setText("");
                resultArea.setText("");
                entropyLabel.setText("Entropy: -");
                lengthLabel.setText("Length: -");
                charsetLabel.setText("Charset: -");
                base64Label.setText("Base64: -");
                hexLabel.setText("Hex: -");
                cipherLabel.setText("Cipher: -");
                chartPanel.repaint();
            }
        });

        setLocationRelativeTo(null);
    }

    private JLabel createMetricLabel(String text) {
        JLabel label = new JLabel(text);
        label.setFont(new Font("SansSerif", Font.BOLD, 14));
        label.setBorder(BorderFactory.createLineBorder(Color.LIGHT_GRAY, 1));
        label.setOpaque(true);
        label.setBackground(new Color(240, 240, 240));
        label.setHorizontalAlignment(SwingConstants.CENTER);
        return label;
    }

    private void analyzeValue() {
        String value = inputArea.getText().trim();
        if (value.isEmpty()) {
            resultArea.setText("Please enter a value to analyze");
            return;
        }

        // 计算熵值
        double entropy = calculateShannonEntropy(value);
        entropyLabel.setText(String.format("Entropy: %.2f", entropy));

        // 计算长度
        lengthLabel.setText("Length: " + value.length());

        // 字符集分析
        String charset = analyzeCharset(value);
        charsetLabel.setText("Charset: " + charset);

        // Base64检测
        boolean isBase64 = isBase64(value);
        base64Label.setText("Base64: " + (isBase64 ? "Yes" : "No"));

        // Hex检测
        boolean isHex = isHex(value);
        hexLabel.setText("Hex: " + (isHex ? "Yes" : "No"));

        // 密文可能性
        boolean likelyCipher = isLikelyCipher(value, entropy);
        cipherLabel.setText("Cipher: " + (likelyCipher ? "Likely" : "Unlikely"));

        // 综合分析
        StringBuilder result = new StringBuilder();
        result.append("===== FIELD VALUE ANALYSIS REPORT =====\n");
        result.append("Input value: ").append(value.length() > 50 ? value.substring(0, 47) + "..." : value).append("\n\n");
        result.append("1. ENTROPY ANALYSIS: ").append(String.format("%.4f bits", entropy)).append("\n");
        result.append("   - ").append(entropy > 4.5 ? "High entropy (likely encrypted data)" :
                entropy > 3.0 ? "Medium entropy (likely encoded data)" : "Low entropy (likely plaintext)").append("\n\n");

        result.append("2. LENGTH CHARACTERISTICS: ").append(value.length()).append(" characters\n");
        result.append("   - ").append(isValidCipherLength(value) ? "Length matches encryption characteristics" : "Length does not match encryption characteristics").append("\n\n");

        result.append("3. CHARSET ANALYSIS: ").append(charset).append("\n");
        result.append("   - Base64: ").append(isBase64 ? "Yes" : "No").append("\n");
        result.append("   - Hexadecimal: ").append(isHex ? "Yes" : "No").append("\n\n");

        result.append("4. OVERALL ASSESSMENT:\n");
        if (likelyCipher) {
            result.append("   - This value is LIKELY SM4 encrypted ciphertext\n");
            result.append("   - Characteristics: High entropy + Valid length + Encoding charset");
        } else if (isLikelyEncoded(value, entropy)) {
            result.append("   - This value is likely encoded data (e.g., Base64 or Hex)\n");
            result.append("   - Characteristics: Medium entropy + Encoding charset");
        } else {
            result.append("   - This value is LIKELY plaintext\n");
            result.append("   - Characteristics: Low entropy + Readable characters");
        }

        result.append("\n\n5. RECOMMENDATIONS:\n");
        if (likelyCipher) {
            result.append("   - Confirm if it's encrypted data; decryption key required for verification");
        } else if (isLikelyEncoded(value, entropy)) {
            result.append("   - Try Base64 or Hex decoding to reveal original data");
        } else {
            result.append("   - No additional processing needed; likely plaintext");
        }

        resultArea.setText(result.toString());
        chartPanel.repaint();
    }

    private void drawChart(Graphics g) {
        String value = inputArea.getText().trim();
        if (value.isEmpty()) {
            return;
        }

        double entropy = calculateShannonEntropy(value);
        boolean isBase64 = isBase64(value);
        boolean isHex = isHex(value);
        boolean validLength = isValidCipherLength(value);

        int width = chartPanel.getWidth();
        int height = chartPanel.getHeight();

        // 清空背景
        g.setColor(Color.WHITE);
        g.fillRect(0, 0, width, height);

        // 绘制标题
        g.setColor(Color.BLACK);
        g.setFont(new Font("SansSerif", Font.BOLD, 14));
        g.drawString("Value Type Probability Analysis", 10, 20);

        // 绘制条形图
        int barWidth = 80;
        int startX = 50;
        int baseY = 80;
        int maxBarHeight = 100;
        int spacing = 20;

        // 明文可能性
        double plainProbability = calculatePlainProbability(value, entropy);
        int plainHeight = (int) (plainProbability * maxBarHeight);
        g.setColor(new Color(70, 130, 180)); // 钢蓝色
        g.fillRect(startX, baseY - plainHeight, barWidth, plainHeight);
        g.drawString("Plaintext", startX, baseY + 20);
        g.drawString(String.format("%.0f%%", plainProbability * 100), startX + 15, baseY + 40);

        // 编码数据可能性
        double encodedProbability = calculateEncodedProbability(value, entropy);
        int encodedHeight = (int) (encodedProbability * maxBarHeight);
        g.setColor(new Color(46, 139, 87)); // 海洋绿
        g.fillRect(startX + barWidth + spacing, baseY - encodedHeight, barWidth, encodedHeight);
        g.drawString("Encoded", startX + barWidth + spacing, baseY + 20);
        g.drawString(String.format("%.0f%%", encodedProbability * 100), startX + barWidth + spacing + 15, baseY + 40);

        // 密文可能性
        double cipherProbability = calculateCipherProbability(value, entropy);
        int cipherHeight = (int) (cipherProbability * maxBarHeight);
        g.setColor(new Color(178, 34, 34)); // 火砖红
        g.fillRect(startX + 2*(barWidth + spacing), baseY - cipherHeight, barWidth, cipherHeight);
        g.drawString("Ciphertext", startX + 2*(barWidth + spacing), baseY + 20);
        g.drawString(String.format("%.0f%%", cipherProbability * 100), startX + 2*(barWidth + spacing) + 15, baseY + 40);

        // 绘制基准线
        g.setColor(Color.GRAY);
        g.drawLine(30, baseY, width - 30, baseY);
    }

    // 熵值计算实现
    public static double calculateShannonEntropy(String input) {
        if (input == null || input.isEmpty()) {
            return 0.0;
        }

        Map<Character, Integer> charCounts = new HashMap<>();
        for (char c : input.toCharArray()) {
            charCounts.put(c, charCounts.getOrDefault(c, 0) + 1);
        }

        double entropy = 0.0;
        int length = input.length();
        for (Map.Entry<Character, Integer> entry : charCounts.entrySet()) {
            double probability = (double) entry.getValue() / length;
            entropy -= probability * (Math.log(probability) / Math.log(2));
        }

        return entropy;
    }

    // 字符集分析
    private String analyzeCharset(String value) {
        boolean hasLetters = false;
        boolean hasDigits = false;
        boolean hasSymbols = false;
        boolean hasNonAscii = false;

        for (char c : value.toCharArray()) {
            if (c >= 'A' && c <= 'Z') hasLetters = true;
            else if (c >= 'a' && c <= 'z') hasLetters = true;
            else if (c >= '0' && c <= '9') hasDigits = true;
            else if (c > 127) hasNonAscii = true;
            else if (!Character.isWhitespace(c)) hasSymbols = true;
        }

        if (hasNonAscii) return "Non-ASCII";
        if (hasLetters && hasDigits && hasSymbols) return "Mixed";
        if (hasLetters && hasDigits) return "Alphanumeric";
        if (hasLetters) return "Alphabetic";
        if (hasDigits) return "Numeric";
        return "Symbols";
    }

    // Base64检测
    private boolean isBase64(String value) {
        if (value.length() % 4 != 0) {
            return false;
        }

        String base64Pattern = "^[A-Za-z0-9+/]+[=]{0,2}$";
        if (!Pattern.matches(base64Pattern, value)) {
            return false;
        }

        try {
            Base64.getDecoder().decode(value);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    // Hex检测
    private boolean isHex(String value) {
        return Pattern.matches("^[0-9a-fA-F]+$", value);
    }

    // 有效加密长度检测
    private boolean isValidCipherLength(String value) {
        int len = value.length();
        // Base64长度：24, 44, 等（16字节->24字符）
        if (isBase64(value) && len % 4 == 0) {
            int decodedLength = (len * 3) / 4;
            if (value.endsWith("==")) decodedLength -= 2;
            else if (value.endsWith("=")) decodedLength -= 1;
            return decodedLength % 16 == 0;
        }
        // Hex长度：32, 64等（16字节->32字符）
        if (isHex(value)) {
            return len % 32 == 0;
        }
        return false;
    }

    // 是否为可能的密文
    private boolean isLikelyCipher(String value, double entropy) {
        return entropy > 4.5 && isValidCipherLength(value);
    }

    // 是否为可能的编码数据
    private boolean isLikelyEncoded(String value, double entropy) {
        return (isBase64(value) || isHex(value)) && entropy > 3.0;
    }

    // 计算明文概率
    private double calculatePlainProbability(String value, double entropy) {
        double prob = 0.0;

        // 低熵值
        if (entropy < 3.0) prob += 0.6;

        // 包含可读单词或常见模式
        if (value.matches(".*\\b(admin|user|test|example|password)\\b.*")) prob += 0.3;

        // 非编码字符集
        if (!isBase64(value) && !isHex(value)) prob += 0.2;

        // 包含空格或常见分隔符
        if (value.contains(" ") || value.contains("@") || value.contains(".")) prob += 0.2;

        return Math.min(0.99, prob);
    }

    // 计算编码数据概率
    private double calculateEncodedProbability(String value, double entropy) {
        double prob = 0.0;

        // 中等熵值
        if (entropy >= 3.0 && entropy <= 4.5) prob += 0.4;

        // 符合编码格式
        if (isBase64(value) || isHex(value)) prob += 0.5;

        // 有效长度但非加密长度
        if (!isValidCipherLength(value) && (value.length() % 4 == 0 || value.length() % 2 == 0)) {
            prob += 0.2;
        }

        return Math.min(0.99, prob);
    }

    // 计算密文概率
    private double calculateCipherProbability(String value, double entropy) {
        double prob = 0.0;

        // 高熵值
        if (entropy > 4.5) prob += 0.6;

        // 符合加密长度
        if (isValidCipherLength(value)) prob += 0.5;

        // 符合编码格式
        if (isBase64(value) || isHex(value)) prob += 0.3;

        // 高随机性
        if (entropy > 5.0) prob += 0.2;

        return Math.min(0.99, prob);
    }

    public static void main(String[] args) {
        // 确保使用正确的字符编码
        System.setProperty("file.encoding", "UTF-8");

        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                try {
                    // 设置跨平台外观
                    UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                new ValueAnalyzer().setVisible(true);
            }
        });
    }
}
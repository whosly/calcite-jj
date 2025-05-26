package com.whosly.calcite.controller;

import com.whosly.calcite.R;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class HelloControllerTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    public void testHelloRest() {
        R<String> rs = restTemplate.getForObject("/hello", R.class);
        System.out.println(rs);

        assertEquals("hello", rs.getData());
    }

    @Test
    public void testHelloRestMap() {
        // 发送 GET 请求到 /hello 端点
        ResponseEntity<Map> response = restTemplate.getForEntity("/hello", Map.class);

        // 验证 HTTP 状态码
        assertEquals(HttpStatus.OK, response.getStatusCode());

        // 验证响应内容
        Map<String, Object> body = response.getBody();
        assertEquals(R.SUCCESS, body.get("code"));
        assertEquals("操作成功", body.get("msg"));
        assertEquals("hello", body.get("data"));
    }

    @Test
    public void testViewIndexEndpoint() {
        String index = restTemplate.getForObject("/", String.class);
        System.out.println(index);

        assertEquals("index", index);
    }
}

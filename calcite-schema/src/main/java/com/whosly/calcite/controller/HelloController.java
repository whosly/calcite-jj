package com.whosly.calcite.controller;

import com.whosly.calcite.R;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Duration;

@RestController
public class HelloController {

    @GetMapping(value = "/hello")
    public Mono<ResponseEntity<R<String>>> hello() {
        return Mono
                .delay(Duration.ofMillis(200))
                .thenReturn(
                        ResponseEntity.ok()
                                .contentType(MediaType.APPLICATION_JSON)
                                .body(R.ok("hello"))
                );
    }

    // 返回值 Mono 或者 String 都行，但是 Mono 代表着我这个返回 View 也是回调的。
    @GetMapping(path = {""})
    public Mono<String> viewIndex(Model model) {
        // Model 对象来进行数据绑定到视图
        model.addAttribute("now", System.currentTimeMillis());

        // return 字符串，该字符串对应的目录在 resources/templates 下的模板名字。
        return Mono.just("index");
    }

}

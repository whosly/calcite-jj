package com.whosly.calcite.controller;

import jakarta.annotation.Resource;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.util.*;

@Component
public class InitResourceCommandLineRunner implements CommandLineRunner, InitializingBean  {
    private static final Logger logger = LoggerFactory.getLogger(InitResourceCommandLineRunner.class);

    /**
     * 上下文
     */
    @Resource
    WebApplicationContext applicationContext;

    @Resource
    private ServerProperties serverProperties;

    @Override
    public void afterPropertiesSet() throws Exception {
        //.
    }

    @Override
    public void run(String... args) throws Exception {
        String ip = "localhost"; // 在实际部署中需要动态获取IP地址
        int port = serverProperties.getPort();

        // 获取controller相关bean
        RequestMappingHandlerMapping mapping = applicationContext.getBean(RequestMappingHandlerMapping.class);

        // 拿到Handler适配器中的全部方法
        Map<RequestMappingInfo, HandlerMethod> methodMap = mapping.getHandlerMethods();
        List<UrlMethod> urlList = new ArrayList<>();
        // 获取methodMap的key集合
        for (Map.Entry<RequestMappingInfo, HandlerMethod> entry : methodMap.entrySet()) {
            RequestMappingInfo info = entry.getKey();
            HandlerMethod method = entry.getValue();

            // controller url集合
            if (info.getPathPatternsCondition() != null) {
                Set<String> urlSet = info.getPathPatternsCondition().getPatternValues();

                // 获取所有方法类型, 全部请求方式
                final Set<RequestMethod> methodSet = info.getMethodsCondition().getMethods();

                urlList.addAll(urlSet.stream().map(url ->
                        UrlMethod.builder().url(url).methodSet(methodSet).build()
                ).toList());

            } else {
                urlList.add(UrlMethod.builder().url(info.getName()).build());
            }
        }
        Collections.sort(urlList);

        logger.info("提供的服务总计 {} 个，清单如下：", urlList.size());
        urlList.forEach(item -> {
            logger.info("{}, http://{}:{}{}", item.methodSet, ip, port, item.url);
        });
    }

    @ToString
    @Getter
    @Setter
    @Builder
    private static class UrlMethod implements Comparable<UrlMethod> {
        private String url;

        private Set<RequestMethod> methodSet;

        @Override
        public int compareTo(UrlMethod o) {
            // 负整数    当前对象的值 < 比较对象的值 ， 位置排在前
            // 零       当前对象的值 = 比较对象的值 ， 位置不变
            // 正整数    当前对象的值 > 比较对象的值 ， 位置排在后

            // 升序排列
            return this.url.compareTo(o.url);
        }
    }
}

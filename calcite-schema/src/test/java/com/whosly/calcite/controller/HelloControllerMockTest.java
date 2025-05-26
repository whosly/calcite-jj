package com.whosly.calcite.controller;

import com.whosly.calcite.R;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Objects;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class HelloControllerMockTest {

    @Mock
    private HelloController controller;

    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testHelloRest() {
        R<String> r = R.ok("AAA");
        Mono<ResponseEntity<R<String>>> rs = Mono
                .delay(Duration.ofMillis(200))
                .thenReturn(
                        ResponseEntity.ok()
                                .contentType(MediaType.APPLICATION_JSON)
                                .body(r)
                );
        when(controller.hello()).thenReturn(rs);

        Assertions.assertEquals(controller.hello(), rs);
        Assertions.assertEquals(Objects.requireNonNull(controller.hello().block()).getBody(), r);
        Assertions.assertEquals(Objects.requireNonNull(Objects.requireNonNull(controller.hello().block()).getBody()).getData(), r.getData());
    }
}

package com.whosly.calcite;

import com.whosly.calcite.controller.HelloController;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

@WebFluxTest(HelloController.class)
public class CalciteApplicationTests {

	@Autowired
	private WebTestClient webTestClient;

	@Test
	public void testHelloEndpoint() {
		webTestClient.get().uri("/hello")
				.accept(MediaType.APPLICATION_JSON)
				.exchange()
				.expectStatus().isOk()
				.expectHeader().contentType(MediaType.APPLICATION_JSON)
				.expectBody()
				.jsonPath("$.code").isEqualTo(200)
				.jsonPath("$.msg").isEqualTo("success")
				.jsonPath("$.data").isEqualTo("hello");
	}
}
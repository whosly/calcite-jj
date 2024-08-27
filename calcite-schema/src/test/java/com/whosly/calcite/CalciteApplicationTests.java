package com.whosly.calcite;

import org.junit.Test;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class CalciteApplicationTests {

	@Autowired
	private TestRestTemplate restTemplate;

	@Test
	public void testRest() {
		R<String> rs = restTemplate.getForObject("/hello", R.class);
		System.out.println(rs);

		Assert.assertEquals("hello", rs.getData());
	}

}

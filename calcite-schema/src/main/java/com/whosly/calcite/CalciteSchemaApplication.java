package com.whosly.calcite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.ServletComponentScan;

import java.util.Arrays;

@SpringBootApplication
@ServletComponentScan
public class CalciteSchemaApplication {
	private static final Logger logger = LoggerFactory.getLogger(CalciteSchemaApplication.class);

	public static void main(String[] args) {
		new SpringApplicationBuilder()
				.main(CalciteSchemaApplication.class)
				.sources(CalciteSchemaApplication.class)
				.run(args);

		System.out.println("(♥◠‿◠)ﾉﾞ  启动成功   ლ(´ڡ`ლ)ﾞ  \n" +
				" .-------.       ____     __        \n" +
				" |  _ _   \\      \\   \\   /  /    \n" +
				" | ( ' )  |       \\  _. /  '       \n" +
				" |(_ o _) /        _( )_ .'         \n" +
				" | (_,_).' __  ___(_ o _)'          \n" +
				" |  |\\ \\  |  ||   |(_,_)'         \n" +
				" |  | \\ `'   /|   `-'  /           \n" +
				" |  |  \\    /  \\      /           \n" +
				" ''-'   `'-'    `-..-'              ");

		logger.info("Application started with args: {}", Arrays.toString(args));
	}

}

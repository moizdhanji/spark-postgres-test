package com.cigna.sees.force.emr.serverless;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration;

@SpringBootApplication(exclude = {GsonAutoConfiguration.class})
public class TestApplication implements CommandLineRunner {
	private static final Logger logger = LogManager.getLogger(TestApplication.class);
	@Value("${amazon.aws.region}")
	private String awsRegion;
	@Value("${amazon.aws.accesskey}")
	private String awsAccessKey;
	@Value("${postgresql.host}")
	private String postgresqlHost;
	@Value("${postgresql.port}")
	private String postgresqlPort;
	@Value("${postgresql.username}")
	private String postgresqlUsername;
	@Value("${postgresql.password}")
	private String postgresqlPassword;
	@Value("${postgresql.database}")
	private String postgresqlDatabase;

	public static void main(String[] args) {
		SpringApplication.run(TestApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		String m = String.format("aws region is %s", awsRegion);
		System.out.println(m);
		logger.info(m);

		try {
			SparkSession spark = SparkSession.builder().master("local[*]").appName("TestEmrServerless").getOrCreate();
			spark.sparkContext().setLogLevel("INFO");
			Dataset<Row> df = spark.read()
					.format("jdbc")
					.option("url", String.format("jdbc:postgresql://%s:%s/%s", postgresqlHost, postgresqlPort, postgresqlDatabase))
					.option("driver", "org.postgresql.Driver")
					.option("dbtable", "public.customer")
					.option("user", postgresqlUsername)
					.option("password", postgresqlPassword)
					.option("partitionColumn", "store_id")
					.option("lowerBound", "1")
					.option("upperBound", "99")
					.option("numPartitions", 4)
					.load();
			df.printSchema();
			logger.info(String.format("total rows %s", df.count()));
			spark.close();
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error(ex.toString());
		}
	}
}

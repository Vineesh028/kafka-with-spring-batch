package com.batch.sample.config;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.kafka.KafkaItemReader;
import org.springframework.batch.item.kafka.builder.KafkaItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.batch.sample.entity.UserEntity;
import com.batch.sample.listener.JobListener;
import com.batch.sample.model.User;
import com.batch.sample.model.UserDeserializer;
import com.batch.sample.repository.UserRepository;
import com.batch.sample.step.DataProcessor;
import com.batch.sample.step.DataWriter;

@Configuration
public class BatchConfig {
	
	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${app.kafka.poll-interval}")
	private Long pollInterval;
	
	@Value("${app.kafka.batch.size}")
	private String batchSize;

	@Value("${app.topic.batch}")
	private String topic;
	
	@Autowired
	private DataProcessor dataProcessor;
	
	@Autowired
	private DataWriter dataWriter;


	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	UserRepository repository;
	

	@Bean
	public Job processJob() {
		return jobBuilderFactory.get("processJob").incrementer(new RunIdIncrementer()).listener(listener())
				.flow(orderStep1()).end().build();
	}

	@Bean
	public Step orderStep1() {
		return stepBuilderFactory.get("orderStep1").<User, UserEntity>chunk(1).reader(kafkaItemReader())
				.processor(dataProcessor).writer(dataWriter).build();
	}

	@Bean
	public JobExecutionListener listener() {
		return new JobListener();
	}

	@Bean
	KafkaItemReader<String, User> kafkaItemReader() {
		Properties props = new Properties();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeserializer.class);
		props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "batch");
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, batchSize);
		//props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, pollInterval);

		return new KafkaItemReaderBuilder<String, User>().partitions(0).consumerProperties(props).name("user-reader")
				.saveState(true).topic(topic).build();
	}
}

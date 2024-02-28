package com.batch.sample.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.batch.item.kafka.builder.KafkaItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.batch.sample.listener.JobListener;
import com.batch.sample.model.User;

@Configuration
public class BatchConfig {

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${app.topic.batch}")
	private String topic;
	
	@Value("${app.batch.filename}")
	private String fileName;

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Bean
	public Job processJob(Step step1) {
		return jobBuilderFactory.get("processJob").incrementer(new RunIdIncrementer()).listener(listener())
				.flow(step1).end().build();
	}

	@Bean
	public Step orderStep1(ItemReader<User> reader) {
		return stepBuilderFactory.get("orderStep1").<User, User>chunk(1).reader(reader).processor(processor())
				.writer(kafkaItemWriter()).build();
	}

	@Bean
	public JobExecutionListener listener() {
		return new JobListener();
	}

//	@Bean
//	public FlatFileItemReader<User> userReader() {
//		return new FlatFileItemReaderBuilder<User>().name("userItemReader").resource(new ClassPathResource("users.csv"))
//				.delimited().delimiter(",").names(new String[] { "id", "userId", "firstName", "lastName", "sex",
//						"email", "phone", "dateofBirth", "jobTitle" })
//				.linesToSkip(1).fieldSetMapper(new BeanWrapperFieldSetMapper<User>() {
//					{
//						setTargetType(User.class);
//					}
//				}).build();
//	}

	@Bean
	public FlatFileItemReader<User> reader(LineMapper<User> lineMapper) {
		FlatFileItemReader<User> itemReader = new FlatFileItemReader<User>();
		itemReader.setLineMapper(lineMapper);
		itemReader.setResource(new ClassPathResource(fileName));
		itemReader.setLinesToSkip(1);
		return itemReader;
	}

	@Bean
	public DefaultLineMapper<User> lineMapper(LineTokenizer tokenizer, FieldSetMapper<User> fieldSetMapper) {
		DefaultLineMapper<User> lineMapper = new DefaultLineMapper<User>();
		lineMapper.setLineTokenizer(tokenizer);
		lineMapper.setFieldSetMapper(fieldSetMapper);
		return lineMapper;
	}

	@Bean
	public BeanWrapperFieldSetMapper<User> fieldSetMapper() {
		var fieldSetMapper = new BeanWrapperFieldSetMapper<User>();
		fieldSetMapper.setTargetType(User.class);
		return fieldSetMapper;
	}

	@Bean
	public DelimitedLineTokenizer tokenizer() {
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setDelimiter(",");
		tokenizer.setNames("id", "userId", "firstName", "lastName", "sex","email", "phone", "dateofBirth", "jobTitle");
		return tokenizer;
	}

	@Bean
	public ItemProcessor<User, User> processor() {
		return user -> {
			return user;
		};
	}

	@Bean
	KafkaItemWriter<String, User> kafkaItemWriter() {
		return new KafkaItemWriterBuilder<String, User>().kafkaTemplate(kafkaTemplate()).itemKeyMapper(User::getId)
				.build();
	}

	@Bean
	public ProducerFactory<String, User> producerFactory() {
		Map<String, Object> config = new HashMap<>();

		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		config.put(ProducerConfig.ACKS_CONFIG, "all");
		config.put(ProducerConfig.RETRIES_CONFIG, 0);
		config.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);
		config.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

		return new DefaultKafkaProducerFactory<String, User>(config);
	}

	@Bean
	public KafkaTemplate<String, User> kafkaTemplate() {
		KafkaTemplate<String, User> template = new KafkaTemplate<>(producerFactory());
		template.setDefaultTopic(topic);
		return template;
	}

}

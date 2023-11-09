package com.eidiko.batch.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import com.eidiko.batch.entity.Customer;
import com.eidiko.batch.repo.CustomerRepo;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private CustomerRepo customerRepo;

	@Bean
	public FlatFileItemReader<Customer> reader(){
		FlatFileItemReader<Customer> itemReader=new FlatFileItemReader<Customer>();
		itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());
		return itemReader;
	}

	private LineMapper<Customer> lineMapper() {
    DefaultLineMapper<Customer> defaultLineMapper=new DefaultLineMapper<Customer>();
    
    //extracting values from Csv file
    DelimitedLineTokenizer delimitedLineTokenizer=new DelimitedLineTokenizer();
    delimitedLineTokenizer.setDelimiter(",");
    delimitedLineTokenizer.setStrict(false);
    delimitedLineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contact", "counrtty", "dob");
    
    //Map values to Customer bean
    BeanWrapperFieldSetMapper<Customer>  fieldSetMapper=new BeanWrapperFieldSetMapper<Customer>();
    fieldSetMapper.setTargetType(Customer.class);
    
    //set DefaultLineMapper
    defaultLineMapper.setLineTokenizer(delimitedLineTokenizer);
    defaultLineMapper.setFieldSetMapper(fieldSetMapper);
    
		return defaultLineMapper;
	}
	
	@Bean
	public CustomerProcessor processor() {
		return new CustomerProcessor();
	}
	
	@Bean
	public RepositoryItemWriter<Customer> writer(){
		RepositoryItemWriter<Customer>  writer=new RepositoryItemWriter<Customer>();
		writer.setRepository(customerRepo);
		writer.setMethodName("save");
		return  writer;
	}
	
	@Bean
	public Step step1() {
		return stepBuilderFactory.get("csv-step")
				.<Customer,Customer>chunk(10)
				.reader(reader())
				.writer(writer())
				.taskExecutor(taskExecutor())
				.build();
	}
	
	@Bean
	public Job runJob() {
		return jobBuilderFactory.get("import-customer")
				.flow(step1())
				.end()
				.build();
	}
	
	@Bean
	public TaskExecutor  taskExecutor() {
		SimpleAsyncTaskExecutor executor=new SimpleAsyncTaskExecutor();
				executor.setConcurrencyLimit(10);
				return executor;
	}

}

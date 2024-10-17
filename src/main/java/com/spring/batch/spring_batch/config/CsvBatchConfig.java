package com.spring.batch.spring_batch.config;

import com.spring.batch.spring_batch.entity.Customer;
import com.spring.batch.spring_batch.repository.CustomerRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
public class CsvBatchConfig {

    @Autowired
    private CustomerRepository customerRepository;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;


    //Step#1 - create Reader
    @Bean
    public FlatFileItemReader<Customer> customerReader(){
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv")); // Giving the file path
        itemReader.setName("csv-reader");  // setting iterm reader Name
        itemReader.setLinesToSkip(1);  // First line skipped from the csv file.
        itemReader.setLineMapper(lineMapper()); // read one line and consider it as Customer object.

        return  itemReader;
    }

    private LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter(","); // csv file data is separated with comma (",")
        tokenizer.setStrict(false); // false:- any column value might not contain value in that case it will take null, True: reverse it, must contain value
        tokenizer.setNames("id", "firstName", "lastName","email", "gender", "contactNo", "country", "dob"); // order of columns names in the file.

        //Now convert data into Bean Customer object
        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }

    //Step#2 - create processor
    @Bean
    public CustomerProcessor customerProcessor(){
        return new CustomerProcessor();
    }

    //Step#3 - create writer
    @Bean
    public RepositoryItemWriter<Customer> customerWriter(){
        RepositoryItemWriter<Customer> repositoryItemWriter = new RepositoryItemWriter<>();
        repositoryItemWriter.setRepository(customerRepository);
        repositoryItemWriter.setMethodName("save");  // this method from repository used to save/insert record the data into DB.

        return repositoryItemWriter;
    }

    //Step#4 - create Step , get(name) any name, <inputType, outputType>chunk() - used to how many records to be processed, reader, processor, writer.
    @Bean
    public Step step(){
        return new StepBuilder("niru", jobRepository).<Customer, Customer>chunk(10, transactionManager)
                .reader(customerReader())
                .processor(customerProcessor())
                .writer(customerWriter())
                .build();
    }

    //Step#5 - create Job , we can configure multiple steps as well under flow()..
    @Bean
    public Job job(){
        return new JobBuilder("csvJob", jobRepository)
                .start(step())
                .build();
    }

}

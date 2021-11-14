package amin.databatch;

import amin.databatch.entity.FileDTO;
import amin.databatch.mapper.DataFieldSetMapper;
import amin.databatch.processor.FlatFileProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;

import java.util.Random;

@SpringBootApplication
@EnableBatchProcessing
@Slf4j
public class DataBatchApplication {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	JobLauncher jobLauncher;

	public static String[] tokens = new String[] {"column_1", "column_2"};

	Random random = new Random();
	int randomWithNextInt = random.nextInt();

	@Bean
	public Job job () throws Exception {
		return this.jobBuilderFactory.get("ExcelFileProcessingJob"+ randomWithNextInt)
				.start(fileProcessingStep())
				.build();
	}

	@Bean
	public Step fileProcessingStep() {
		return this.stepBuilderFactory.get("readFileStep")
				.<FileDTO, FileDTO>chunk(1)
				.reader(fileReader())
				.processor(fileProcessor())
				.writer(items -> log.debug("item writer"))
				.build();
	}

	@Bean
	public ItemProcessor<FileDTO, FileDTO> fileProcessor() {
		return new FlatFileProcessor();
	}

	//reading data from csv
	@Bean
	public FlatFileItemReader<FileDTO> fileReader() {
		FlatFileItemReader itemReader = new FlatFileItemReader<FileDTO>();
		itemReader.setLinesToSkip(1);
		itemReader.setResource(new FileSystemResource("C:\\Users\\Abdim\\Desktop\\JAVA\\data-batch\\src\\main\\resources\\files\\testFile.csv"));

		DefaultLineMapper<FileDTO> lineMapper = new DefaultLineMapper<FileDTO>();
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setNames(tokens);

		//set tokenizer on line mapper
		lineMapper.setLineTokenizer(tokenizer);

		lineMapper.setFieldSetMapper(new DataFieldSetMapper());

		itemReader.setLineMapper(lineMapper);
		return itemReader;
	}

	public static void main(String[] args) {
		SpringApplication.run(DataBatchApplication.class, args);
	}

}

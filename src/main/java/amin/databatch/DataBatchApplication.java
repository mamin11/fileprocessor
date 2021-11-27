package amin.databatch;

import amin.databatch.api.FileStorageService;
import amin.databatch.entity.FileDTO;
import amin.databatch.mapper.DataFieldSetMapper;
import amin.databatch.processor.FlatFileProcessor;
import amin.databatch.service.StringHeaderWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.util.Random;

@SpringBootApplication
@EnableBatchProcessing
@Slf4j
public class DataBatchApplication implements CommandLineRunner {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	JobLauncher jobLauncher;

	@javax.annotation.Resource
	FileStorageService fileStorageService;

	public static final String[] tokens = new String[] {"column_1", "column_2"};


	Random random = new Random();
	int randomWithNextInt = random.nextInt();

	public Resource outPutResource = new FileSystemResource("output/processedFile"+ randomWithNextInt +".csv");

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
				.reader(fileReader("test.csv"))
				.processor(fileProcessor())
				.writer(flatFileWriter())
				.build();
	}

	@Bean
	public ItemWriter<FileDTO> flatFileWriter() {
		FlatFileItemWriter<FileDTO> writer = new FlatFileItemWriter<>();
		writer.setResource(outPutResource);
		writer.setAppendAllowed(true);

		//Name field values sequence based on object properties
		writer.setLineAggregator(fileDTOLineAggregator());

		StringHeaderWriter headerWriter = new StringHeaderWriter("column_1, column_2, sum");
		writer.setHeaderCallback(headerWriter);

		return writer;
	}

	@Bean
	public LineAggregator<FileDTO> fileDTOLineAggregator() {
		DelimitedLineAggregator<FileDTO> lineAggregator = new DelimitedLineAggregator<>();
		lineAggregator.setDelimiter(",");

		FieldExtractor<FileDTO> fieldExtractor = createFileFieldsExtractor();
		lineAggregator.setFieldExtractor(fieldExtractor);

		return lineAggregator;
	}

	@Bean
	public FieldExtractor<FileDTO> createFileFieldsExtractor() {
		BeanWrapperFieldExtractor<FileDTO> extractor = new BeanWrapperFieldExtractor<>();
		extractor.setNames(new String[] {
				"column_1", "column_2", "sum"
		});
		return extractor;
	}

	@Bean
	public ItemProcessor<FileDTO, FileDTO> fileProcessor() {
		return new FlatFileProcessor();
	}

	//reading data from csv
	@Bean
	public FlatFileItemReader<FileDTO> fileReader(String filename) {
		FlatFileItemReader itemReader = new FlatFileItemReader<FileDTO>();
		itemReader.setLinesToSkip(1);
		itemReader.setResource(new FileSystemResource("uploads/" + filename + ".csv"));

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

	@Override
	public void run(String... args) throws Exception {
		fileStorageService.deleteAll();
		fileStorageService.init();
	}
}

package amin.databatch;

import amin.databatch.api.FileStorageService;
import amin.databatch.entity.FileDTO;
import amin.databatch.entity.UploadedFile;
import amin.databatch.mapper.DataFieldSetMapper;
import amin.databatch.mapper.UploadedFileMapper;
import amin.databatch.processor.FlatFileProcessor;
import amin.databatch.repository.UploadedFileRepository;
import amin.databatch.service.StringHeaderWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import javax.sql.DataSource;
import java.util.Optional;
import java.util.Random;

@SpringBootApplication
@EnableBatchProcessing
@Slf4j
public class DataBatchApplication implements CommandLineRunner {

	//TODO:
	// - remove predefined columns
	// - add support for more mathematical operations
	// - store files in s3

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	JobLauncher jobLauncher;

	@javax.annotation.Resource
	FileStorageService fileStorageService;

	@Autowired
	DataSource dataSource;

	@Autowired
	UploadedFileRepository uploadedFileRepository;

	public static final String[] tokens = new String[] {"column_1", "column_2"};


	Random random = new Random();
	int randomWithNextInt = random.nextInt();

	public Resource outPutResource = new FileSystemResource("output/processedFile"+ randomWithNextInt +".csv");

	@Bean(name = "ExcelFileProcessingJob")
	@Scheduled(fixedRate = 2000)
	public Job job () throws Exception {
		return this.jobBuilderFactory.get("ExcelFileProcessingJob")
				.start(fileProcessingStep())
				.build();
//		return this.jobBuilderFactory.get("ExcelFileProcessingJob"+ randomWithNextInt)
	}

	@Bean
	public Step fileProcessingStep() {
		return this.stepBuilderFactory.get("readFileStep")
				.<FileDTO, FileDTO>chunk(1)
				.reader(fileReader(null))
				.processor(fileProcessor())
				.writer(flatFileWriter())
				.listener(myStepListerner())
				.build();
	}

	private StepExecutionListener myStepListerner() {
		return new StepExecutionListener() {
			@Override
			public void beforeStep(StepExecution stepExecution) {
				log.debug("Before step");
			}

			@Override
			public ExitStatus afterStep(StepExecution stepExecution) {
				log.debug("after step");
				if (stepExecution.getExitStatus() != ExitStatus.FAILED) {
					// mark file as processed
					String filename = stepExecution.getJobParameters().getString("filename");
					Optional<UploadedFile> optionalUploadedFile = uploadedFileRepository.findByFilename(filename);
					if (!optionalUploadedFile.isEmpty()) {
						UploadedFile file = optionalUploadedFile.get();
						file.setProcessed(true);
					}
				}
				return ExitStatus.COMPLETED;
			}
		};
	}

	@Bean
	public ItemWriter<FileDTO> flatFileWriter() {
		log.debug("writing file");
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
	@StepScope
	public FlatFileItemReader<FileDTO> fileReader(@Value("#{jobParameters['filename']}") String filename) {
		FlatFileItemReader itemReader = new FlatFileItemReader<FileDTO>();
		itemReader.setLinesToSkip(1);
		itemReader.setResource(new FileSystemResource("uploads/" + filename));

		DefaultLineMapper<FileDTO> lineMapper = new DefaultLineMapper<FileDTO>();
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setNames(tokens);

		//set tokenizer on line mapper
		lineMapper.setLineTokenizer(tokenizer);

		lineMapper.setFieldSetMapper(new DataFieldSetMapper());

		itemReader.setLineMapper(lineMapper);
		return itemReader;
	}

	@Bean
	public PagingQueryProvider queryProvider() throws Exception {
		SqlPagingQueryProviderFactoryBean factoryBean = new SqlPagingQueryProviderFactoryBean();
		factoryBean.setSelectClause("select *");
		factoryBean.setFromClause("from uploaded_file");
		factoryBean.setSortKey("id");
		factoryBean.setDataSource(dataSource);
		return factoryBean.getObject();
	}

	@Bean
	public ItemReader<UploadedFile> dbReader() throws Exception {
		String sql = "SELECT * FROM uploaded_file";

		return new JdbcCursorItemReaderBuilder<UploadedFile>()
				.name("reader")
				.dataSource(dataSource)
				.sql(sql)
				.rowMapper(new UploadedFileMapper())
				.build();
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

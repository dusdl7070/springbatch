package hhy.study.springbatch.job.chunk.flow;

import hhy.study.springbatch.common.listener.JobListener;
import hhy.study.springbatch.common.listener.StepListener;
import hhy.study.springbatch.job.NumberVO;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.util.Random;

/**
 * @Purpose         : 흐름+분기를 가진 Spring Batch 형태
 * @Date            : 2020.03
 * @Author          : 한혜연
 * @Comment         : 복잡도가 중간인 배치 프로그램에서 사용할 것, success/fail 에 따른 분기가 가능, 조건에 따라서 분기 가능
 */

@Configuration
@EnableBatchProcessing
public class DecideJobConfig {
    public final String JOB_NAME = "DecideJob";

    JobBuilderFactory jobBuilderFactory;
    StepBuilderFactory stepBuilderFactory;

    @Autowired
    public DecideJobConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    /**
     * 1. validator
     * 2. step -> success/fail or if -> step -> ...
     */

    @Bean
    public JobParametersValidator decideJobParamsValidator() {
        String[] requiredKeys = new String[]{"requiredVal"};
        String[] optionalKeys = new String[]{"executeTime"};
        return new DefaultJobParametersValidator(requiredKeys, optionalKeys);
    }

    @Bean
    public JobExecutionDecider decider() {
        return new JobExecutionDecider() {
            @Override
            public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
                Random random = new Random();

                int randomNum = random.nextInt(50) + 1;

                String result = "";
                if (randomNum % 2 == 0) {
                    result = "FAIL";
                } else {
                    result = "SUCCESS";
                }

                return new FlowExecutionStatus(result);
            }
        };
    }

    @Bean
    public Job decideJob() throws Exception {
        return jobBuilderFactory.get(JOB_NAME)
                .validator(decideJobParamsValidator())
                .start(decideJobStep())
                .next(decider())
                .from(decider())
                    .on("FAIL")
                    .end()
                .from(decider())
                .on("SUCCESS")
                .to(decidedJobStep())
                .end()
                .listener(new JobListener())
                .build();
    }

    @Bean
    @JobScope
    public Step decideJobStep () {
        return stepBuilderFactory.get("decideJobStep")
                .<NumberVO, NumberVO>chunk(5)
                .reader(decideItemReader(null))
                .writer(decideItemWriter())
                .listener(new StepListener())
                .build();

    }

    @Bean
    @StepScope
    public FlatFileItemReader<NumberVO> decideItemReader(@Value("#{jobParameters['requiredVal']}") String requiredVal) {

        FlatFileItemReader<NumberVO> fileItemReader = new FlatFileItemReader<>();
        Resource resource = new ClassPathResource(requiredVal);
        if (resource.exists()) {
            System.out.println("file exist.");
        } else {
            System.out.println("file not exist.");
        }

        fileItemReader.setResource(resource);

        DefaultLineMapper<NumberVO> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter("|");
        lineTokenizer.setNames(new String[] {"key","num"});

        lineMapper.setLineTokenizer(lineTokenizer);

        BeanWrapperFieldSetMapper<NumberVO> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(NumberVO.class);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        fileItemReader.setLineMapper(lineMapper);
        fileItemReader.setLinesToSkip(0);

        return fileItemReader;
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<NumberVO> decideItemWriter() {
        Resource outputResource = new FileSystemResource("data/dflow-step1/output.csv");

        FlatFileItemWriter<NumberVO> fileItemWriter = new FlatFileItemWriter<>();
        fileItemWriter.setResource(outputResource);

        fileItemWriter.setAppendAllowed(true);

        fileItemWriter.setLineAggregator(new DelimitedLineAggregator<NumberVO>(){
            {
                setDelimiter("@@");
                setFieldExtractor(new BeanWrapperFieldExtractor<NumberVO>(){
                    {
                        setNames(new String[]{"key", "num"});
                    }
                });
            }

        });

        return fileItemWriter;
    }

    @Bean
    @JobScope
    public Step decidedJobStep () {
        return stepBuilderFactory.get("successJobStep")
                .<NumberVO, NumberVO>chunk(5)
                .reader(decidedItemReader(null))
                .writer(decidedItemWriter())
                .listener(new StepListener())
                .build();

    }

    @Bean
    @StepScope
    public FlatFileItemReader<NumberVO> decidedItemReader(@Value("#{jobParameters['requiredVal']}") String requiredVal) {

        FlatFileItemReader<NumberVO> fileItemReader = new FlatFileItemReader<>();
        Resource resource = new ClassPathResource(requiredVal);
        if (resource.exists()) {
            System.out.println("file exist.");
        } else {
            System.out.println("file not exist.");
        }

        fileItemReader.setResource(resource);

        DefaultLineMapper<NumberVO> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter("|");
        lineTokenizer.setNames(new String[] {"key","num"});

        lineMapper.setLineTokenizer(lineTokenizer);

        BeanWrapperFieldSetMapper<NumberVO> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(NumberVO.class);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        fileItemReader.setLineMapper(lineMapper);
        fileItemReader.setLinesToSkip(0);

        return fileItemReader;
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<NumberVO> decidedItemWriter() {
        Resource outputResource = new FileSystemResource("data/dflow-success/output.csv");

        FlatFileItemWriter<NumberVO> fileItemWriter = new FlatFileItemWriter<>();
        fileItemWriter.setResource(outputResource);

        fileItemWriter.setAppendAllowed(true);

        fileItemWriter.setLineAggregator(new DelimitedLineAggregator<NumberVO>(){
            {
                setDelimiter("**");
                setFieldExtractor(new BeanWrapperFieldExtractor<NumberVO>(){
                    {
                        setNames(new String[]{"key", "num"});
                    }
                });
            }

        });

        return fileItemWriter;
    }
}

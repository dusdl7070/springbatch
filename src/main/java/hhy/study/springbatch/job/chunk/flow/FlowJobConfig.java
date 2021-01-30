package hhy.study.springbatch.job.chunk.flow;

import hhy.study.springbatch.common.listener.JobListener;
import hhy.study.springbatch.common.listener.StepListener;
import hhy.study.springbatch.job.NumberVO;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
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

/**
 * @Purpose         : 흐름을 가진 Spring Batch 형태
 * @Date            : 2020.03
 * @Author          : 한혜연
 * @Comment         : 복잡도가 중간인 배치 프로그램에서 사용할 것, success/fail 에 따른 분기가 가능,
 *                    fail 상황을 control 할 경우 커스텀해서 사용하자
 */

@Configuration
@EnableBatchProcessing
public class FlowJobConfig {
    public final String JOB_NAME = "FlowJob";

    JobBuilderFactory jobBuilderFactory;
    StepBuilderFactory stepBuilderFactory;

    @Autowired
    public FlowJobConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    /**
     * 1. validator
     * 2. step -> success/fail -> step -> ...
     */

    @Bean
    public JobParametersValidator flowJobParamsValidator() {
        String[] requiredKeys = new String[]{"requiredVal"};
        String[] optionalKeys = new String[]{"executeTime"};
        return new DefaultJobParametersValidator(requiredKeys, optionalKeys);
    }

    @Bean
    public Job flowJob() throws Exception {
        return jobBuilderFactory.get(JOB_NAME)
                .validator(flowJobParamsValidator())
                .start(flowJobStep())
                    .on("FAILD")
                    .end()
                .from(flowJobStep())
                    .on("*")
                    .to(successJobStep())
                    .on("*")
                .end()
                .end()
                .listener(new JobListener())
                .build();
    }

    @Bean
    @JobScope
    public Step flowJobStep () {
        return stepBuilderFactory.get("flowJobStep")
                .<NumberVO, NumberVO>chunk(5)
                .reader(flowItemReader(null))
                .writer(flowItemWriter())
                .listener(new StepListener())
                .build();

    }

    @Bean
    @StepScope
    public FlatFileItemReader<NumberVO> flowItemReader(@Value("#{jobParameters['requiredVal']}") String requiredVal) {

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
    public FlatFileItemWriter<NumberVO> flowItemWriter() {
        Resource outputResource = new FileSystemResource("data/flow-step1/output.csv");

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
    public Step successJobStep () {
        return stepBuilderFactory.get("successJobStep")
                .<NumberVO, NumberVO>chunk(5)
                .reader(successItemReader(null))
                .writer(successItemWriter())
                .listener(new StepListener())
                .build();

    }

    @Bean
    @StepScope
    public FlatFileItemReader<NumberVO> successItemReader(@Value("#{jobParameters['requiredVal']}") String requiredVal) {

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
    public FlatFileItemWriter<NumberVO> successItemWriter() {
        Resource outputResource = new FileSystemResource("data/flow-success/output.csv");

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

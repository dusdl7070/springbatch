package hhy.study.springbatch.job.chunk.simple;

import hhy.study.springbatch.common.listener.JobListener;
import hhy.study.springbatch.common.listener.StepListener;
import hhy.study.springbatch.job.NumberVO;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.*;

/**
 * @Purpose         : 가장 기본적인 Spring Batch 형태
 * @Date            : 2020.03
 * @Author          : 한혜연
 * @Comment         : 복잡도가 낮은 간단한 배치 프로그램에서 사용할 것
 * @Disadvantages   : 배치의 성공/실패에 따른 분기는 불가능 하기 때문에 배치 특성 고려하여 결정할 것
 */

@Configuration
@EnableBatchProcessing
public class SimpleJobConfig {
    public final String JOB_NAME = "SimpleJob";

    JobBuilderFactory jobBuilderFactory;
    StepBuilderFactory stepBuilderFactory;

    public SimpleJobConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;

    }

    /**
     * 1. validator
     * 2. step -> step
     */

    @Bean
    public JobParametersValidator simpleJobValidator() {
        String[] requiredKeys = new String[]{"requiredVal"};
        String[] optionalKeys = new String[]{"executeTime"};
        return new DefaultJobParametersValidator(requiredKeys, optionalKeys);
    }

    @Bean
    public Job simpleJob() throws Exception {
        return jobBuilderFactory.get(JOB_NAME)
                .validator(simpleJobValidator())
                .start(simpleJobStep())
                .listener(new JobListener())
                .build();
    }

    @Bean
    @JobScope
    public Step simpleJobStep () {
        return stepBuilderFactory.get("simpleJobStep")
                .<NumberVO, NumberVO>chunk(5)
                .reader(simpleItemReader(null))
                .processor(simpleItemProcessor())
                .writer(simpleItemWriter())
                .listener(new StepListener())
                .build();

    }

    @Bean
    @StepScope
    public FlatFileItemReader<NumberVO> simpleItemReader(@Value("#{jobParameters['requiredVal']}") String requiredVal) {

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
    public ItemProcessor<NumberVO, NumberVO> simpleItemProcessor() { // Generic : input, output 객체
        // processor에 비즈니스 로직이 들어가는 경우는 별도 클래스로 컴포넌트화 해서 관리
        return new ItemProcessor<NumberVO, NumberVO>() {
            @Override
            public NumberVO process(NumberVO numberVO) throws Exception {
                System.out.println(numberVO.getNum());

                return numberVO;
            }
        };
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<NumberVO> simpleItemWriter() {
        Resource outputResource = new FileSystemResource("data/simple/output.csv");

        FlatFileItemWriter<NumberVO> fileItemWriter = new FlatFileItemWriter<>();
        fileItemWriter.setResource(outputResource);

        fileItemWriter.setAppendAllowed(true);

        fileItemWriter.setLineAggregator(new DelimitedLineAggregator<NumberVO>(){
            {
                setDelimiter("@");
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

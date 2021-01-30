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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.*;

/**
 * @Purpose         : 가장 기본적인 Spring Batch 형태
 * @Date            : 2020.03
 * @Author          : 한혜연
 * @Comment         : 가장 기본적인 형태
 * @Disadvantages   : 배치의 성공/실패에 따른 분기는 불가능 하기 때문에 배치 특성 고려하여 결정할 것
 */

@Configuration
@EnableBatchProcessing
public class SimpleJobConfig {
    public final String JOB_NAME = "SimpleJob";

    JobBuilderFactory jobBuilderFactory;
    StepBuilderFactory stepBuilderFactory;

    @Autowired
    public SimpleJobConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        /* 각각 클래스별 @Autowired를 할수는 있지만 spring 보안문제로 권장방식은 생성자 내 Autowired 방식
        *  2020.04 전부 생성자 @Autowired 방식으로 변경했음. */
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;

    }

    /**
     * 1. validator
     * 2. step -> step
     */

    @Bean
    public JobParametersValidator simpleJobValidator() {
        /* 배치의 받는 파라미터 유효성검사 */
        String[] requiredKeys = new String[]{"requiredVal"};
        String[] optionalKeys = new String[]{"executeTime"};
        return new DefaultJobParametersValidator(requiredKeys, optionalKeys);
    }

    @Bean
    public Job simpleJob() throws Exception {
        /* 배치의 시작점 */
        return jobBuilderFactory.get(JOB_NAME)
                .validator(simpleJobValidator())
                .start(simpleJobStep())
                .listener(new JobListener())
                .build();
    }

    @Bean
    @JobScope
    public Step simpleJobStep () {
        /* chunk, reader, processor, writer를 등록해준다. 이때 chunk는 나머지 전 단계에 영향을 끼치므로 상황에 따라서 적절하게 설정. */
        /* listener : 작업의 전/후 과정에 실행되는 클래스 */
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

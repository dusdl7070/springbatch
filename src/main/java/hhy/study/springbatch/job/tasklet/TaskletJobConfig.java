package hhy.study.springbatch.job.tasklet;

import hhy.study.springbatch.common.listener.JobListener;
import hhy.study.springbatch.common.listener.StepListener;
import hhy.study.springbatch.job.NumberVO;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.MethodInvokingTaskletAdapter;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

/**
 * @Purpose         : 단일 세부 작업 단위 Spring Batch 형태, 거의 사용하지 않는 추세
 * @Date            : 2020.03
 * @Author          : 한혜연
 * @Comment         : 복잡하고/중요할수록/정형화될수록 CHUNK 방식 선택할 것
 */

@Configuration
@EnableBatchProcessing
public class TaskletJobConfig {
    public final String JOB_NAME = "TaskletJob";

    JobBuilderFactory jobBuilderFactory;
    StepBuilderFactory stepBuilderFactory;

    @Autowired
    public TaskletJobConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;

    }

    /**
     * 1. validator
     * 2. step(tasklet) -> step(tasklet)
     */

    @Bean
    public JobParametersValidator taskletJobValidator() {
        String[] requiredKeys = new String[]{"requiredVal"};
        String[] optionalKeys = new String[]{"executeTime"};
        return new DefaultJobParametersValidator(requiredKeys, optionalKeys);
    }

    @Bean
    public Job taskletJob() throws Exception {
        return jobBuilderFactory.get(JOB_NAME)
                .validator(taskletJobValidator())
                .start(taskletJobStep1())
                .listener(new JobListener())
                .build();
    }

    @Bean
    @JobScope
    public Step taskletJobStep1 () {
        return stepBuilderFactory.get("taskletJobStep1")
                .tasklet(tasklet1())
                .listener(new StepListener())
                .build();

    }

    @Bean
    public MethodInvokingTaskletAdapter tasklet1() {
        MethodInvokingTaskletAdapter adapter = new MethodInvokingTaskletAdapter();
//        adapter.setTargetObject(); -> Dao 입력
//        adapter.setTargetMethod(); -> 업데이트 쿼리

        return adapter;
    }

    @Bean
    @JobScope
    public Step taskletJobStep2 () {
        return stepBuilderFactory.get("taskletJobStep2")
                .tasklet(tasklet1())
                .listener(new StepListener())
                .build();

    }

    @Bean
    public Tasklet tasklet2() {
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                for (int i=0; i<=100; i++) {
                    System.out.println("i -> " + i);
                }

                return RepeatStatus.FINISHED;
            }
        };
    }

}

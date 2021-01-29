package hhy.study.springbatch;

import hhy.study.springbatch.job.chunk.simple.SimpleJobConfig;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {SimpleJobConfig.class})
@SpringBootTest
class SimpleJobApplicationTest {

    Job job;
    JobLauncher jobLauncher;
    JobRepository jobRepository;

    @Autowired
    public SimpleJobApplicationTest(@Qualifier("simpleJob") Job job, JobLauncher jobLauncher, JobRepository jobRepository) {
        this.job = job;
        this.jobLauncher = jobLauncher;
        this.jobRepository = jobRepository;
    }

    public JobLauncherTestUtils jobLauncherTestUtils() {
        JobLauncherTestUtils testUtils = new JobLauncherTestUtils();
        testUtils.setJob(job);
        testUtils.setJobLauncher(jobLauncher);
        testUtils.setJobRepository(jobRepository);
        return testUtils;
    }

    @Test
    void simpleJobTest() throws Exception{
        Random random = new Random();
        random.setSeed(System.currentTimeMillis());

        Map<String, JobParameter> map = new HashMap<>();
        map.put("requiredVal", new JobParameter("/input.txt"));
        map.put("executeTime", new JobParameter(random.nextLong()));
        JobParameters parameters = new JobParameters(map);

        jobLauncherTestUtils().launchJob(parameters).getStatus();
    }

}

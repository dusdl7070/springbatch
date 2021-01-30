package hhy.study.springbatch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @Purpose : Spring Batch Framework
 * @date    : 2020.03
 * @Author  : 한혜연
 * @Test    : Junit
 */
@EnableScheduling
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class SpringbatchApplication {

    /**
     * @@@@SUMMERY
     * 1. SimpleJobConfig : 기본 형태
     * 2. FlowJobConfig   : 기본 형태 + success/fail 처리
     * 3. DecideJobConfig : 기본 형태 + success/fail + if 처리
     * 내려갈수록 커스텀 범위와 활용할 수 있는 범위가 넓어짐
     *
     * [삽질 History]
     * 2020.03, epopcon job manage issue
     * -> 이관 ~ 정제 ~ 적제 ~ 엑셀생성
     *    다양한 이유로 단독 요청이 잦으므로 각 단계를 Job으로 구성해서 job parameter로 로컬대신 서버에서 돌린 단계를 선택하도록 개발하고 싶었음
     *    job끼리는 순서보장이 되지 않음(치명적). 순차적으로 job이 실행되는 코드를 개발하였음. 추후 이런 유사한 개발을 해야한다면 삽질하지말고
     *    순차적으로 개발되도록 코드를 짜서 관리하는 것이 심신의 건강에 좋을 것 같음.
     * 2020.04, epopcon migration issue
     * -> batch job 순차적으로 실행되도록 커스텀 후 배치 속도 이슈로 멀티쓰레드로 변경했을때
     *    Reader쪽 쓰레드가 꼬이는 이슈 발생, 해당 부분 모두 커스텀으로 최상위단 implements 받아서 개발 진행하였음
     *    멀티쓰레드 동기화를 유지시키는 부분에 커스텀 들어감. @configuration, @bean 특성 참조
     *    + 배치 프로그램은 죽더라도 멈추면 안된다. 그렇지만 정확한 오류 추적은 가능해야함. Exception을 최대한 줄이고 방어로직을 강화할 것
     * 2020.06 epopcon migration issue
     * -> Step 과 Step 사이에서 이벤트를 발생시키고자 한다면 synchronized 맹신하지말고 listener 를 커스텀 하자, before/after
     *    implements 받아서 사용하면 step과 step 사이에 배치 내부적으로 각 단계가 섞이는걸 차단시켜줌.
     *    + Bean 적절하게 사용해야함. 이 부분을 놓쳐서 운영중인 배치프로그램 리팩토링을 진행하였음.
     * 2020.10, epopcon emart (txt -> db)
     * -> source 가 분산되어 있는 경우 chunk로 개량하려 했으나 한계점 발생
     *    개발 검토 결과 Job이 싱글톤으로 존재, Step은 동적으로 source 갯수 만큼 생성되어야 했음. (배치 실행 각 단계에서 무지해서 발생)
     *    Job은 1개, Step내에서 tasklet을 source 갯수만큼 동적으로 생성하여 해결하였음.
     */

    public static void main(String[] args) {
        SpringApplication.run(SpringbatchApplication.class, args);
    }

}

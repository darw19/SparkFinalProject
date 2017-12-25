package football;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.apache.spark.sql.Dataset;

@Aspect
@Component
@Log4j
@Profile("dev")
public class ShowDatasetAspect {

    @SneakyThrows
    @Around("execution(* football.ETL.data_preparation.*.*(..))")
    public Dataset print(ProceedingJoinPoint point) {
        Dataset dataset = (Dataset) point.proceed();
        dataset.show();
        return dataset;
    }
}

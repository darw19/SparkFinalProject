package football;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

@Configuration
@ComponentScan
@EnableAspectJAutoProxy
@PropertySource("classpath:user.properties")
public class Helper {

    @Autowired
    private SparkConf sparkConfiguration;
    @Bean
    public JavaSparkContext sparkContext(){
        return new JavaSparkContext(sparkConfiguration);
    }
    @Bean
    public SQLContext sqlContext(){
        return new SQLContext(sparkContext());
    }
}

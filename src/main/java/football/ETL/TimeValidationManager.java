package football.ETL;
import football.UDFAnnotation;
import lombok.extern.log4j.Log4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.api.java.UDF1;
import org.springframework.stereotype.Component;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import java.io.Serializable;


@UDFAnnotation(returnType = "boolean")
@Component
@Log4j
public class TimeValidationManager implements FootballValidationManager, UDF1<String,Boolean>, Serializable {

    @Override
    public Boolean call(String eventTime) throws Exception {

        String[] values = eventTime.split(":");
        int mins = Integer.parseInt(values[0]);

        if (mins < 130) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Dataset validate(Dataset dataset) {
        return dataset.filter(callUDF(TimeValidationManager.class.getName(), (col("startTime"))))
                .filter(callUDF(TimeValidationManager.class.getName(), (col("eventTime"))));
    }
}

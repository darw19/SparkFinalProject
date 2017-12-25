package football.ETL;
import football.UDFAnnotation;
import lombok.extern.log4j.Log4j;
import org.springframework.stereotype.Component;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.api.java.UDF1;
import java.io.Serializable;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

@UDFAnnotation(returnType = "integer")
@Component
@Log4j
public class HalfTimeEnrichmentManager implements FootballEnrichmentManager, UDF1<String,Integer>, Serializable {

    @Override
    public Integer call(String eventTime) throws Exception {
        String[] values = eventTime.split(":");
        int mins = Integer.parseInt(values[0]);
        return mins < 49 ? 1 : 2;
    }

    @Override
    public Dataset enrich(Dataset dataset) {
        return dataset.withColumn("half", callUDF(HalfTimeEnrichmentManager.class.getName(), (col("eventTime"))));
    }
}

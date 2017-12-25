package football.ETL;
import football.UDFAnnotation;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.Properties;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.api.java.UDF1;

@UDFAnnotation(returnType = "string")
@Component
@Log4j
public class DescriptionEnrichmentManager implements FootballEnrichmentManager, UDF1<Integer,String>, Serializable {

    private Properties codeDescriptions;

    @PostConstruct
    @SneakyThrows
    private void loadPlayers() {
        this.codeDescriptions = PropertiesLoaderUtils.loadAllProperties("codes.properties");
    }

    @Override
    public String call(Integer code) throws Exception {
        return codeDescriptions.getProperty(code.toString());
    }

    @Override
    public Dataset enrich(Dataset dataset) {
        return dataset.withColumn("codeDescription", callUDF(DescriptionEnrichmentManager.class.getName(), (col("code"))));
    }
}

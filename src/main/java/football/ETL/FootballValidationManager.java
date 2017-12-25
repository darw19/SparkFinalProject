package football.ETL;
import org.apache.spark.sql.Dataset;

public interface FootballValidationManager {

    Dataset validate(Dataset dataset);
}

package football.ETL;
import org.apache.spark.sql.Dataset;

public interface FootballEnrichmentManager {

    Dataset enrich(Dataset dataset);
}

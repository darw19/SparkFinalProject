package football.ETL.data_preparation;
import football.ETL.FootballEnrichmentManager;
import football.ETL.FootballValidationManager;
import org.apache.spark.sql.Dataset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
public class DataPreparator {


    @Autowired(required=false)
    private List<FootballEnrichmentManager> enrichmnentManagers;
    @Autowired(required=false)
    private List<FootballValidationManager> validationManagers;

    public Dataset prepareData(Dataset datasetToProcess){

        for (FootballValidationManager validator : validationManagers) {
            datasetToProcess = validator.validate(datasetToProcess);
        }

        for (FootballEnrichmentManager enricher : enrichmnentManagers) {
            datasetToProcess = enricher.enrich(datasetToProcess);
        }

        return datasetToProcess;
    }
}

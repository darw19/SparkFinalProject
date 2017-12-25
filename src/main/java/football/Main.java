package football;
import football.ETL.data_preparation.DataPreparator;
import football.ETL.data_preparation.DataLoader;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.apache.spark.sql.Dataset;

public class Main {

    public static void main(String[] args) {

        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Helper.class);

        DataPreparator businessLogic = context.getBean(DataPreparator.class);

        DataLoader dataLoader = context.getBean(DataLoader.class);

        Dataset dataset = dataLoader.load();

        dataset = businessLogic.prepareData(dataset);
    }
}

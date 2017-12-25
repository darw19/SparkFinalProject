package football.ETL.data_preparation;
import football.Event;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DataLoader {

    @Autowired
    private transient JavaSparkContext sc;

    @Autowired
    private transient SQLContext sqlContext;

    public Dataset load() {
        JavaRDD<String> rdd = sc.textFile("data/football/rawData.txt");
        //only non-empty rows
        JavaRDD<Event> rowRDD = rdd.filter(line -> !line.isEmpty()).map(line -> getEventFromLine(line));

        return sqlContext.createDataFrame(rowRDD, Event.class);
    }


    private static Event getEventFromLine(String text) {
        String[] splitData = text.split(";");
        String[] values = new String[splitData.length];

        for (int i = 0; i < splitData.length; i++) {
            values[i] = splitData[i].split("=")[1];
        }

        Event event = new Event();

        event.setCode(Integer.parseInt(values[0]));
        event.setNameFrom(values[1]);
        event.setNameTo(values[2]);
        event.setEventTime(values[3]);
        event.setStadium(values[4]);
        event.setStartTime(values[5]);

        return event;
    }

}

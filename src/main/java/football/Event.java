package football;
import lombok.Data;

@Data
public class Event {

    private String descr;
    private Integer code;
    private String nameFrom;
    private String nameTo;
    private String eventTime;
    private String stadium;
    private String startTime;
}

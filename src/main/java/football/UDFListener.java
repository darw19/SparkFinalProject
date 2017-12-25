package football;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;
import java.util.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import java.util.Collection;

@Component
public class UDFListener implements ApplicationListener<ContextRefreshedEvent> {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private SQLContext sqlContext;

    private static final Map<String, DataType> typeMap = createMap();
    private static Map<String, DataType> createMap()
    {
        Map<String,DataType> myMap = new HashMap<String,DataType>();
        myMap.put("string", DataTypes.StringType);
        myMap.put("integer", DataTypes.IntegerType);
        myMap.put("boolean", DataTypes.BooleanType);

        return myMap;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        Collection<Object> udfObjects = context.getBeansWithAnnotation(UDFAnnotation.class).values();
        for (Object udfObject : udfObjects) {
            sqlContext.udf().register(udfObject.getClass().getName(), (UDF1<?, ?>) udfObject,
                    typeMap.get(udfObject.getClass().getAnnotation(UDFAnnotation.class).returnType()));
        }
    }
}

package goncharov.hkbTest.handler;

import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.*;

public class GlobalTemperatureHandlerTest extends JavaDatasetSuiteBase implements Serializable {

    private final String globalDataPath = "C:/Users/VLAD/Desktop/HCB/GlobalTemperatures.csv";
    private GlobalTemperatureHandler globalHandler;

    @Before
    public void before() {
        Dataset<Row> globalData = sqlContext().read().option("header", true).csv(globalDataPath);
        globalHandler = new GlobalTemperatureHandler(globalData);
    }

    @Test
    public void initializationInitData() {
    }

    @Test
    public void setDataForYear() {
    }

    @Test
    public void setDataForDecade() {
    }

    @Test
    public void setDataForCentury() {
    }

    @Test
    public void setFinalData() {
    }
}
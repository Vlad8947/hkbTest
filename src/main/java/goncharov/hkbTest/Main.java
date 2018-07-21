package goncharov.hkbTest;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

import javax.xml.crypto.Data;
import java.net.URI;

public class Main {

    private static final String cityDataURI = "";
    private static final String countryDataURI = "";
    private static final String globalDataURI = "";
    private static final String endingDataURI = "hdfs://192.168.0.63:8020/user/cloudera/WorldReport.parquet";

    private static final String headSchema = "City,Country,dt," +
        /*City, year*/          "AverageCityTemperatureForYear,MinCityTemperatureForYear,MaxCityTemperatureForYear," +
        /*City, 10years*/       "AverageCityTemperatureForTenYears,MinCityTemperatureForTenYears,MaxCityTemperatureForTenYears," +
        /*City, century*/       "AverageCityTemperatureForCentury,MinCityTemperatureForCentury,MaxCityTemperatureForCentury," +

        /*Country, year*/       "AverageCountryTemperatureForYear,MinCountryTemperatureForYear,MaxCountryTemperatureForYear," +
        /*Country, 10years*/    "AverageCountryTemperatureForTenYears,MinCountryTemperatureForTenYears,MaxCountryTemperatureForTenYears," +
        /*Country, century*/    "AverageCountryTemperatureForCentury,MinCountryTemperatureForCentury,MaxCountryTemperatureForCentury," +

        /*Global, year*/        "AverageGlobalTemperatureForYear,MinGlobalTemperatureForYear,MaxGlobalTemperatureForYear," +
        /*Global, 10years*/     "AverageGlobalTemperatureForTenYears,MinGlobalTemperatureForTenYears,MaxGlobalTemperatureForTenYears," +
        /*Global, century*/     "AverageGlobalTemperatureForCentury,MinGlobalTemperatureForCentury,MaxGlobalTemperatureForCentury";


    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        SparkSession sparkSession = SparkSession.builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Encoder<String> encoder = Encoders.STRING();

        Dataset<Row> endingData;

        try {
            Dataset<Row> cityData = sparkSession.read().csv(cityDataURI);
            Dataset<Row> countryData = sparkSession.read().csv(countryDataURI);
            Dataset<Row> globalData = sparkSession.read().csv(globalDataURI);



//            endingData.write().parquet(endingDataURI);
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        finally {
            sparkSession.close();
            sparkContext.close();
        }
    }

}

package goncharov.hkbTest.test;

import goncharov.hkbTest.handler.*;
import org.apache.commons.collections.functors.FactoryTransformer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.collection.generic.SeqFactory;
import scala.collection.mutable.ArraySeq;
import scala.collection.parallel.ParIterableLike;

import java.util.*;
import java.util.stream.Collectors;

public class Test {

    private static SparkConf sparkConf;
    private static JavaSparkContext sparkContext;
    private static SparkSession sparkSession;

    private static final String cityPath = "C:/Users/VLAD/Desktop/HCB/GlobalLandTemperaturesByCity.csv";
    private static final String countryPath = "C:/Users/VLAD/Desktop/HCB/GlobalLandTemperaturesByCountry.csv";
    private static final String globalPath = "C:/Users/VLAD/Desktop/HCB/GlobalTemperatures.csv";

    private static int firstYear;
    private static int lastYear;
    private static int year;

    public static void main(String[] args) {

        sparkConf = new SparkConf().setAppName("Temperature_data_handler").setMaster("local[*]");
        sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLogLevel("WARN");

        sparkSession = SparkSession.builder()
                .appName("Temperature_data_handler")
                .config("spark.some.config.option", "option-value")
                .getOrCreate();

        try {
            long t1 = System.currentTimeMillis();

            testGlobal();

            System.out.println((float) (System.currentTimeMillis() - t1)/1000/60);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        sparkSession.close();
        sparkContext.close();
    }

    private static void readTest() {
        Dataset<Row> data = sparkSession.read().option("header", true).csv(globalPath);
        data.write().parquet("C:/Users/VLAD/Desktop/HCB/writeTest.parquet");
        sparkSession.read().parquet("C:/Users/VLAD/Desktop/HCB/writeTest.parquet").show();
    }

    public static void mainTest() {
        Dataset<Row> cityData, countryData, globalData;
        cityData = sparkSession.read().option("header", true).csv(cityPath);
        countryData = sparkSession.read().option("header", true).csv(countryPath);
        globalData = sparkSession.read().option("header", true).csv(globalPath);

        MainTemperatureHandler mainHandler = new MainTemperatureHandler(cityData, countryData, globalData);
        Dataset<Row> finalData = mainHandler.handleAndGetFinalData();
        finalData.sort(DataHandler.getStrYear()).show();
    }

    private static void testGlobal() {
        Dataset<Row> globalData = sparkSession.read().option("header", true).csv(globalPath);
        GlobalTemperatureHandler handler = new GlobalTemperatureHandler(globalData);

        globalData
                .sort(DataHandler.getStrDt())
                .show(50);
        handler
                .handleAndGetFinalData()
                .sort(DataHandler.getStrDt())
                .show();
    }

    private static void testCountry() {
        Dataset<Row> countryData = sparkSession.read().option("header", true).csv(countryPath);
        CountryTemperatureHandler handler = new CountryTemperatureHandler(countryData);
        handler.goTest();
    }

    private static void testCity() {
        Dataset<Row> cityData = sparkSession.read().option("header", true).csv(cityPath);
        CityTemperatureHandler handler = new CityTemperatureHandler(cityData);
        handler.goTest();
    }

    private static void test_1(){

        DF1 d1 = new DF1(1, "my");
        Encoder<DF1> df1Encoder = Encoders.bean(DF1.class);
        Dataset<DF1> df1 = sparkSession.createDataset(Collections.singletonList(d1), df1Encoder);

        DF1 d11 = new DF1(1, "my12");
        Dataset<DF1> df11 = sparkSession.createDataset(Collections.singletonList(d11), df1Encoder);

        Dataset<DF1> df1Dataset = sparkSession.createDataset(Collections.singletonList(new DF12(1, "try")), df1Encoder);

//        d1.setList(new ArrayList<>());
//        d1.getList().add(d1);
//        d1.getList().add(d11);

        df1Dataset.show();

        DF2 d2 = new DF2(2, "test1", "my");
        Encoder<DF2> df2Encoder = Encoders.bean(DF2.class);
        Dataset<DF2> df2 = sparkSession.createDataset(Collections.singletonList(d2), df2Encoder);

        ArraySeq<String> seq = new ArraySeq<>(1);
        seq.update(0, "num");
//        seq.update(1, "value");


//      'inner', 'outer', 'full', 'fullouter', 'leftouter', 'left', 'rightouter', 'right', 'leftsemi', 'leftanti', 'cross'

        Dataset<Row> df = df1.join(df2, seq, "full").join(df1Dataset, seq, "full");
        df.show();

        df.groupBy("num").count().show();
//        df.filter(df.col("value").like(null)).show();

    }

}





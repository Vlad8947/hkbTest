package goncharov.hkbTest.test;

import goncharov.hkbTest.handler.CityTempHandler;
import goncharov.hkbTest.handler.GlobalTempHandler;
import goncharov.hkbTest.handler.SchemaHandler;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import scala.Int;
import scala.collection.mutable.ArraySeq;

import java.util.*;

public class Test {

    private static SparkConf sparkConf;
    private static JavaSparkContext sparkContext;
    private static SparkSession sparkSession;

    private static int firstYear;
    private static int lastYear;
    private static int year;

    public static void main(String[] args) {

        sparkConf = new SparkConf().setAppName("test").setMaster("local[*]");
        sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLogLevel("WARN");

        sparkSession = SparkSession.builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value2")
                .getOrCreate();

        try {

//        test_1();
            test_5();

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        sparkSession.close();
        sparkContext.close();
    }

    private static void test_5 () throws InterruptedException {
        long t1 = System.currentTimeMillis();
        String s = "";
        
        Dataset<Row> cityData = sparkSession.read().csv("C:/Users/VLAD/Desktop/HCB/GlobalLandTemperaturesByCity.csv");
        CityTempHandler handler = new CityTempHandler(cityData.sqlContext());
        handler.process(cityData);

        System.out.println((float) (System.currentTimeMillis() - t1)/1000/60);

    }

    private static void test_4() {

        Dataset<Row> cityData = sparkSession.read().csv("C:/Users/VLAD/Desktop/HCB/GlobalLandTemperaturesByCity.csv");

        firstYear = lastYear = 1800;
        long t1 = System.currentTimeMillis();
        SchemaHandler.setSchemaFromCsv(cityData).select("dt").foreach(
                row -> {
                    year = Integer.parseInt(row.getString(0).split("-")[0]);
                    if (year < firstYear) firstYear = year;
                    if (year > lastYear) lastYear = year;
                }
        );
        System.out.println(System.currentTimeMillis() - t1);
        System.out.println(firstYear + " " + lastYear);

//        int f = Integer.parseInt(set.select(strDt).groupBy().min("value").first().<String>getAs(0).split("-")[0]);

    }

    private static void test_3(){

        Dataset<Row> cityData = sparkSession.read().csv("C:/Users/VLAD/Desktop/HCB/GlobalLandTemperaturesByCity.csv");
        cityData = SchemaHandler.setSchemaFromCsv(cityData);
        cityData = cityData.sort("City", "dt").select("City", "Country", "dt", "AverageTemperature");
        cityData = cityData.filter(cityData.col("AverageTemperature").isNotNull());
        int dt = cityData.first().fieldIndex("dt");
        String year = cityData.first().getString(dt).split("-")[0];
        System.out.println("String = " + year);
        System.out.println("1743-05-05.startsWith(1743) = " + new String("1744-05-05").startsWith(year));
    }

    private static void test_2(){

        /** Изменить схему */
        Dataset<Row> cityData = sparkSession.read().csv("C:/Users/VLAD/Desktop/HCB/GlobalLandTemperaturesByCity.csv");
        cityData = SchemaHandler.setSchemaFromCsv(cityData);
        Dataset<Row> dtData = cityData.select("dt");
        cityData.show(5);
        dtData.show(5);
        dtData = dtData.join(cityData.select("City", "dt"), "dt");
        dtData.show(5);
//        cityData = SchemaHandler.setSchemaFromCsv(cityData);
//        cityData.show(10);
//        cityData.write().parquet("C:/Users/VLAD/Desktop/HCB/myParq");
//
//        sparkSession.read().parquet("C:/Users/VLAD/Desktop/HCB/myParq").show(10);


//        cityData = cityData.select(cityData.col("_c0").cast("my"));
//        cityData.printSchema();
//        cityData.write().parquet("C:/Users/VLAD/Desktop/HCB/myParq");

//        List<Row> list = cityData.collectAsList();
//        sparkSession.createDataFrame(list, Row.class).printSchema();

//        cityData = cityData.select("_c3","_c4","_c0", "_c1");
//        cityData.show();
//        cityData.sort("_c3","_c4","_c0").show();

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





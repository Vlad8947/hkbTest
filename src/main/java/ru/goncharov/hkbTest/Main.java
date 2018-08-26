package ru.goncharov.hkbTest;

import ru.goncharov.hkbTest.handlers.TemperatureHandler;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

/**
 *  Класс - точка запуска.
 *  Описывается конфигурирование сеанса, запуск обработчика, запись выходных данных.
 */
public class Main {

    /** Указание путей для чтения и записи данных */
    private static final String clouderaPath = "hdfs://192.168.0.63:8020/user/cloudera";
    private static final String finalParquetDataPath = clouderaPath + "/hkb_test/FinalTemperature.parquet";
    private static final String cityPath = "C:/HCB/GlobalLandTemperaturesByCity.csv";
    private static final String countryPath = "C:/HCB/GlobalLandTemperaturesByCountry.csv";
    private static final String globalPath = "C:/HCB/GlobalTemperatures.csv";

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Temperature-data-handlers").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLogLevel("WARN");
        SparkSession sparkSession = SparkSession.builder()
                .appName("Temperature-data-handlers")
                .config("spark.some.config.option", "option-value")
                .getOrCreate();
        try {
            handleData(sparkSession);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        finally {
            sparkSession.close();
            sparkContext.close();
        }
    }

    /** Метод инкапсулирует процессы считывания исходных данных,
     * передачу и запуск обработчика, запись конечных данных */
    private static void handleData(SparkSession sparkSession) {
        Dataset<Row> cityData = sparkSession.read().option("header", true).csv(cityPath);
        Dataset<Row> countryData = sparkSession.read().option("header", true).csv(countryPath);
        Dataset<Row> globalData = sparkSession.read().option("header", true).csv(globalPath);

        TemperatureHandler temperatureHandler = new TemperatureHandler(cityData, countryData, globalData);
        Dataset<Row> finalData = temperatureHandler.handleAndGetFinalData();
        finalData.write().option("header", true).parquet(finalParquetDataPath);
    }
}

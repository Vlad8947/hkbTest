package goncharov.hkbTest.handler;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

public class CityTempHandler extends DataHandler {

    private static Dataset<Row> initData;
    private static List<Row> forYearList;
    private static Dataset<Row> citiesData;
    private static Dataset<String> yearsData;
    private static Seq<String> avTempStrSeq;

    private static String city;
    private static String country;
    private static String year;
    private static float yearAverageTemp;

    private static int firstYear;
    private static int lastYear;
//    private static int year;
    private static float tempSum = 0;
    private static int monthSum = 0;


    public CityTempHandler(SQLContext sqlContext) {
        super(sqlContext);
        strAverageTemperature = "AverageTemperature";

        strAverageTemperatureForYear = "AverageCityTemperatureForYear";
        strMinTemperatureForYear = "MinCityTemperatureForYear";
        strMaxTemperatureForYear = "MaxCityTemperatureForYear";

        strAverageTemperatureForTenYears = "AverageCityTemperatureForTenYears";
        strMinTemperatureForTenYears = "MinCityTemperatureForTenYears";
        strMaxTemperatureForTenYears = "MaxCityTemperatureForTenYears";

        strAverageTemperatureForTenCentury = "AverageCityTemperatureForCentury";
        strMinTemperatureForTenCentury = "MinCityTemperatureForCentury";
        strMaxTemperatureForTenCentury = "MaxCityTemperatureForCentury";
    }

    public Dataset<Row> process(Dataset<Row> data) {
        initData = SchemaHandler.setSchemaFromCsv(data);
        initData = initData.filter(initCol(strAverageTemperature).isNotNull())
                .select(initCol(strDt), initCol(strCity), initCol(strCountry), initCol(strAverageTemperature).cast(DataTypes.FloatType));
        initData.persist(StorageLevel.MEMORY_ONLY());

        initData.printSchema();

        citiesData = initData.select(strCity, strCountry).dropDuplicates();

//        long t1 = System.currentTimeMillis();

        yearsData = initData.select(strDt).map(
                row -> row.getString(0).split("-")[0] ,
                Encoders.STRING()
        ).dropDuplicates();

//        System.out.println(System.currentTimeMillis() - t1);
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField(strDt, DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField(strCity, DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField(strCountry, DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField(strAverageTemperatureForYear, DataTypes.FloatType, true));
        StructType forYearStructType = DataTypes.createStructType(structFields);

//        forYearList = sqlContext.createDataFrame(new ArrayList<Row>(), structType);
//        forYearList.printSchema();

//        String year = "1744";
//        String city = "Århus";

//        averageTemperature = initData
//                .filter(initCol(strCity).like(city))
//                .filter(initCol(strDt).startsWith(year))
//                .groupBy()
//                .mean(strAverageTemperature)
//                .toDF(strAverageTemperatureForYear);
//
//        avTempStrSeq = new ArraySeq<>(1);
//        ((ArraySeq<String>) avTempStrSeq).update(0, strAverageTemperatureForYear);
//
//        forYearList.join(averageTemperature, avTempStrSeq, "full").show();

        forYearList = new ArrayList<>();

        yearsData.persist(StorageLevel.MEMORY_ONLY());
        citiesData.persist(StorageLevel.MEMORY_ONLY());

        Dataset<Row> years = yearsData.select(yearsData.col("value").as(strDt));

        initData = initData.join(years, initData.col(strDt)
                .startsWith(years.col(strDt)))
                .drop(initCol(strDt));
        initData.persist(StorageLevel.MEMORY_ONLY());
        initData.groupBy(initCol(strCity), initCol(strCountry), initCol(strDt)).mean(strAverageTemperature)
                .show();


//        yearsData.foreach(
//                yearStr -> {
//
//                    year = "1800";
//        citiesData.foreach(
//                city_country -> {
//
//                    city = city_country.<String>getAs(strCity);
//                    country = city_country.<String>getAs(strCountry);
//
//                    yearAverageTemp =
//                            initData.filter(initCol(strCity).like(city))
//                                    .filter(initCol(strDt).startsWith(year))
//                                    .groupBy()
//                                    .mean(strAverageTemperature)
//                                    .first()
//                                    .getFloat(0);
//
//                    forYearList.add(
//                            RowFactory.create(
//                                    city,
//                                    country,
//                                    year,
//                                    yearAverageTemp
//                            )
//                    );
//
//                }
//
//        );
//
//                }
//        );

//        city_country -> {
//
//            city = city_country.<String>getAs(strCity);
//            country = city_country.<String>getAs(strCountry);
//
//            yearAverageTemp =
//                    initData.filter(initCol(strCity).like(city))
//                            .filter(initCol(strDt).startsWith(year))
//                            .groupBy()
//                            .mean(strAverageTemperature)
//                            .first()
//                            .getFloat(0);
//
//            forYearList.add(
//                    RowFactory.create(
//                            city,
//                            country,
//                            year,
//                            yearAverageTemp
//                    )
//            );
//
//        }

//        System.out.println(forYearList.get(0).toString());
//        sqlContext.createDataFrame(forYearList, forYearStructType).show();

        return null;
    }

    private Column initCol(String name) {
        return initData.col(name);
    }

}

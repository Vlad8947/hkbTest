package goncharov.hkbTest.handler;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple3;
import scala.collection.Seq;
import scala.collection.mutable.ArraySeq;
import scala.collection.mutable.ListBuffer;
import scala.collection.mutable.StringBuilder;
import scala.sys.process.ProcessBuilderImpl;

import java.sql.Struct;
import java.util.*;

public class CityTempHandler extends DataHandler {

    public CityTempHandler(SQLContext sqlContext) {
        super(sqlContext);
        strAverageTemperature = "AverageTemperature";

        strAverageTemperatureForYear = "AverageCityTemperatureForYear";
        strMinTemperatureForYear = "MinCityTemperatureForYear";
        strMaxTemperatureForYear = "MaxCityTemperatureForYear";

        strAverageTemperatureForDecade = "AverageCityTemperatureForDecade";
        strMinTemperatureForDecade = "MinCityTemperatureForDecade";
        strMaxTemperatureForDecade = "MaxCityTemperatureForDecade";

        strAverageTemperatureForCentury = "AverageCityTemperatureForCentury";
        strMinTemperatureForCentury = "MinCityTemperatureForCentury";
        strMaxTemperatureForCentury = "MaxCityTemperatureForCentury";
    }

    public Dataset<Row> process(Dataset<Row> initData) {
        initData = SchemaHandler.setSchemaFromCsv(initData);
        initData =
                initData
                        .filter(initData.col(strAverageTemperature).isNotNull())
                        .select(
                                initData.col(strDt),
                                initData.col(strCity),
                                initData.col(strCountry),
                                initData.col(strAverageTemperature).cast(DataTypes.FloatType)
                        );

        initData.persist(StorageLevel.MEMORY_ONLY());

        initData.printSchema();

        Dataset<Row> yearsData =
                initData.map(
                        (MapFunction<Row, Tuple3<String, String, String>>)
                                row -> {
                                    String year = row.getString(0).split("-")[0];
                                    String yearOfTen = year.substring(0, 3);
                                    String century = year.substring(0, 2);
                                    return new Tuple3(year, yearOfTen, century);
                                }, Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.STRING()))
                        .toDF(strYear, strDecade, strCentury)
                        .dropDuplicates(strYear);

        initData =
                initData
                        .join(yearsData, initData.col(strDt).startsWith(yearsData.col(strYear)));
//        initData.sort(strCity, strYear).show();

        Dataset<Row> dataForYear =
                initData
                        .groupBy(
                                initData.col(strCity),
                                initData.col(strCountry),
                                initData.col(strYear)
                        )
                        .mean(strAverageTemperature)
                        .toDF(strCity, strCountry, strYear, strAverageTemperatureForYear)
                        .join(yearsData, strYear);
//        dataForYear.sort(strCity, strYear).show();

        Dataset<Row> dataForDecade =
                dataForYear
                        .groupBy(
                                dataForYear.col(strCity),
                                dataForYear.col(strDecade)
                        ).mean(strAverageTemperatureForYear)
                        .toDF(strCity, strDecade, strAverageTemperatureForDecade)
                        .join(yearsData.select(strDecade, strCentury).dropDuplicates(strDecade), strDecade);
//        dataForDecade.sort(strCity, strDecade).show();

        Dataset<Row> dataForCentury =
                dataForDecade
                        .groupBy(
                                dataForDecade.col(strCity),
                                dataForDecade.col(strCentury)
                        ).mean(strAverageTemperatureForDecade)
                        .toDF(strCity, strCentury, strAverageTemperatureForCentury);
//        dataForCentury.sort(strCity, strCentury).show();

        ArraySeq<String> decadeColumns = new ArraySeq<>(2);
        decadeColumns.update(0, strCity);
        decadeColumns.update(1, strDecade);

        ArraySeq<String> centuryColumns = new ArraySeq<>(2);
        centuryColumns.update(0, strCity);
        centuryColumns.update(1, strCentury);

        Dataset<Row> finalData =
                dataForYear
                        .join(
                                dataForDecade.select(strCity, strDecade, strAverageTemperatureForDecade),
                                decadeColumns)
                        .join(
                                dataForCentury.select(strCity, strCentury, strAverageTemperatureForCentury),
                                centuryColumns);

        finalData.sort(strCity, strYear).show();

        return finalData;
    }

}

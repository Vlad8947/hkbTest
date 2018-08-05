package goncharov.hkbTest.handler;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple3;
import scala.collection.mutable.ArraySeq;

public class CityTemperHandler extends DataHandler {

    public CityTemperHandler(Dataset<Row> data) {
        super(data);
    }

    @Override
    protected void setColumnNames(){
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

    @Override
    protected void setSeqColumns() {
        int length = 3;

        yearColumns = new ArraySeq<>(length);
        yearColumns.update(0, strCity);
        yearColumns.update(1, strCountry);
        yearColumns.update(2, strYear);

        decadeColumns = new ArraySeq<>(length);
        decadeColumns.update(0, strCity);
        decadeColumns.update(1, strCountry);
        decadeColumns.update(2, strDecade);

        centuryColumns = new ArraySeq<>(length);
        centuryColumns.update(0, strCity);
        centuryColumns.update(1, strCountry);
        centuryColumns.update(2, strCentury);
    }

    @Override
    protected void setInitData(Dataset<Row> data) {
        data = SchemaHandler.setSchemaFromCsv(data);
        initData =
                data.filter(data.col(strAverageTemperature).isNotNull())
                        .select(
                                data.col(strDt),
                                data.col(strCity),
                                data.col(strCountry),
                                data.col(strAverageTemperature).cast(DataTypes.FloatType)
                        );
    }

    @Override
    protected void setDataForYear() {
        RelationalGroupedDataset yearGroup =
                initData.groupBy(
                        initData.col(strCity),
                        initData.col(strCountry),
                        initData.col(strYear)
                );

        Dataset<Row> yearAvTemp =
                yearGroup
                        .mean(strAverageTemperature)
                        .toDF(strCity, strCountry, strYear, strAverageTemperatureForYear)
                        .dropDuplicates(strCity, strCountry, strYear);
        Dataset<Row> yearMinTemp =
                yearGroup
                        .min(strAverageTemperature)
                        .toDF(strCity, strCountry, strYear, strMinTemperatureForYear);
        Dataset<Row> yearMaxTemp =
                yearGroup
                        .max(strAverageTemperature)
                        .toDF(strCity, strCountry, strYear, strMaxTemperatureForYear);

        yearData =
                yearAvTemp
                        .join(yearMinTemp, yearColumns)
                        .join(yearMaxTemp, yearColumns)
                        .join(yearsList, strYear);
    }

    @Override
    protected void setDataForDecade() {
        RelationalGroupedDataset decadeGroup =
                yearData.groupBy(
                        yearData.col(strCity),
                        yearData.col(strCountry),
                        yearData.col(strDecade)
                );

        Dataset<Row> decadeAvTemp =
                decadeGroup
                        .mean(strAverageTemperatureForYear)
                        .toDF(strCity, strCountry, strDecade, strAverageTemperatureForDecade);
        Dataset<Row> decadeMinTemp =
                decadeGroup
                        .min(strAverageTemperatureForYear)
                        .toDF(strCity, strCountry, strDecade, strMinTemperatureForDecade);
        Dataset<Row> decadeMaxTemp =
                decadeGroup
                        .max(strAverageTemperatureForYear)
                        .toDF(strCity, strCountry, strDecade, strMaxTemperatureForDecade);

        decadeData =
                decadeAvTemp
                        .join(decadeMinTemp, decadeColumns)
                        .join(decadeMaxTemp, decadeColumns)
                        .join(yearsList.select(strDecade, strCentury).dropDuplicates(strDecade), strDecade);
    }

    @Override
    protected void setDataForCentury() {
        RelationalGroupedDataset centuryGroup =
                decadeData.groupBy(
                        decadeData.col(strCity),
                        decadeData.col(strCountry),
                        decadeData.col(strCentury)
                );

        Dataset<Row> centuryAvTemp =
                centuryGroup
                        .mean(strAverageTemperatureForDecade)
                        .toDF(strCity, strCountry, strCentury, strAverageTemperatureForCentury);
        Dataset<Row> centuryMinTemp =
                centuryGroup
                        .min(strAverageTemperatureForDecade)
                        .toDF(strCity, strCountry, strCentury, strMinTemperatureForCentury);
        Dataset<Row> centuryMaxTemp =
                centuryGroup
                        .max(strAverageTemperatureForDecade)
                        .toDF(strCity, strCountry, strCentury, strMaxTemperatureForCentury);

        centuryData =
                centuryAvTemp
                        .join(centuryMinTemp, centuryColumns)
                        .join(centuryMaxTemp, centuryColumns);
    }

    @Override
    protected Dataset<Row> getFinalData() {
        return yearData
                .join(decadeData.drop(strCentury), decadeColumns)
                .join(centuryData, centuryColumns);
    }

    public void goTest() { // Antwerp, Bangalore, Copiapo
        initData =
                initData.filter(initData.col(strCity).like("Antwerp"));

//        initData.show();

        processAndGetData()
                .printSchema()
//                .sort(strCity, strYear)
//                .show()
        ;
    }

}

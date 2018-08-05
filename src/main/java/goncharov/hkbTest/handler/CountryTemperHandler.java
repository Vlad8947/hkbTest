package goncharov.hkbTest.handler;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.mutable.ArraySeq;

public class CountryTemperHandler extends DataHandler {

    public CountryTemperHandler(Dataset<Row> data) {
        super(data);

    }

    @Override
    protected void setColumnNames() {
        strAverageTemperature = "AverageTemperature";

        strAverageTemperatureForYear = "AverageCountryTemperatureForYear";
        strMinTemperatureForYear = "MinCountryTemperatureForYear";
        strMaxTemperatureForYear = "MaxCountryTemperatureForYear";

        strAverageTemperatureForDecade = "AverageCountryTemperatureForTenYears";
        strMinTemperatureForDecade = "MinCountryTemperatureForTenYears";
        strMaxTemperatureForDecade = "MaxCountryTemperatureForTenYears";

        strAverageTemperatureForCentury = "AverageCountryTemperatureForCentury";
        strMinTemperatureForCentury = "MinCountryTemperatureForCentury";
        strMaxTemperatureForCentury = "MaxCountryTemperatureForCentury";
    }

    @Override
    protected void setSeqColumns() {
        int length = 2;

        yearColumns = new ArraySeq<>(length);
        yearColumns.update(0, strCountry);
        yearColumns.update(1, strYear);

        decadeColumns = new ArraySeq<>(length);
        decadeColumns.update(0, strCountry);
        decadeColumns.update(1, strDecade);

        centuryColumns = new ArraySeq<>(length);
        centuryColumns.update(0, strCountry);
        centuryColumns.update(1, strCentury);
    }

    @Override
    protected void setInitData(Dataset<Row> data) {
        data = SchemaHandler.setSchemaFromCsv(data);
        initData =
                data.filter(data.col(strAverageTemperature).isNotNull())
                        .select(
                                data.col(strDt),
                                data.col(strCountry),
                                data.col(strAverageTemperature).cast(DataTypes.FloatType)
                        );
    }

    @Override
    protected void setDataForYear() {
        RelationalGroupedDataset yearGroup =
                initData.groupBy(
                        initData.col(strCountry),
                        initData.col(strYear)
                );

        Dataset<Row> yearAvTemp =
                yearGroup
                        .mean(strAverageTemperature)
                        .toDF(strCountry, strYear, strAverageTemperatureForYear)
                        .dropDuplicates(strCountry, strYear);
        Dataset<Row> yearMinTemp =
                yearGroup
                        .min(strAverageTemperature)
                        .toDF(strCountry, strYear, strMinTemperatureForYear);
        Dataset<Row> yearMaxTemp =
                yearGroup
                        .max(strAverageTemperature)
                        .toDF(strCountry, strYear, strMaxTemperatureForYear);

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
                        yearData.col(strCountry),
                        yearData.col(strDecade)
                );

        Dataset<Row> decadeAvTemp =
                decadeGroup
                        .mean(strAverageTemperatureForYear)
                        .toDF(strCountry, strDecade, strAverageTemperatureForDecade);
        Dataset<Row> decadeMinTemp =
                decadeGroup
                        .min(strAverageTemperatureForYear)
                        .toDF(strCountry, strDecade, strMinTemperatureForDecade);
        Dataset<Row> decadeMaxTemp =
                decadeGroup
                        .max(strAverageTemperatureForYear)
                        .toDF(strCountry, strDecade, strMaxTemperatureForDecade);

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
                        decadeData.col(strCountry),
                        decadeData.col(strCentury)
                );

        Dataset<Row> centuryAvTemp =
                centuryGroup
                        .mean(strAverageTemperatureForDecade)
                        .toDF(strCountry, strCentury, strAverageTemperatureForCentury);
        Dataset<Row> centuryMinTemp =
                centuryGroup
                        .min(strAverageTemperatureForDecade)
                        .toDF(strCountry, strCentury, strMinTemperatureForCentury);
        Dataset<Row> centuryMaxTemp =
                centuryGroup
                        .max(strAverageTemperatureForDecade)
                        .toDF(strCountry, strCentury, strMaxTemperatureForCentury);

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
}

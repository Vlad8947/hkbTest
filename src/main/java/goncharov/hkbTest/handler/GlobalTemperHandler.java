package goncharov.hkbTest.handler;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.mutable.ArraySeq;

public class GlobalTemperHandler extends DataHandler {

    public GlobalTemperHandler(Dataset<Row> data) {
        super(data);
    }

    @Override
    protected void setColumnNames() {
        strAverageTemperature = "LandAverageTemperature";

        strAverageTemperatureForYear = "AverageGlobalTemperatureForYear";
        strMinTemperatureForYear = "MinGlobalTemperatureForYear";
        strMaxTemperatureForYear = "MaxGlobalTemperatureForYear";

        strAverageTemperatureForDecade = "AverageGlobalTemperatureForTenYears";
        strMinTemperatureForDecade = "MinGlobalTemperatureForTenYears";
        strMaxTemperatureForDecade = "MaxGlobalTemperatureForTenYears";

        strAverageTemperatureForCentury = "AverageGlobalTemperatureForCentury";
        strMinTemperatureForCentury = "MinGlobalTemperatureForCentury";
        strMaxTemperatureForCentury = "MaxGlobalTemperatureForCentury";
    }

    @Override
    protected void setSeqColumns() {
        int length = 1;

        yearColumns = new ArraySeq<>(length);
        yearColumns.update(0, strYear);

        decadeColumns = new ArraySeq<>(length);
        decadeColumns.update(0, strDecade);

        centuryColumns = new ArraySeq<>(length);
        centuryColumns.update(0, strCentury);
    }

    @Override
    protected void setInitData(Dataset<Row> data) {
        data = SchemaHandler.setSchemaFromCsv(data);
        initData =
                data.filter(data.col(strAverageTemperature).isNotNull())
                        .select(
                                data.col(strDt),
                                data.col(strAverageTemperature).cast(DataTypes.FloatType)
                        );
    }

    @Override
    protected void setDataForYear() {
        RelationalGroupedDataset yearGroup =
                initData.groupBy(
                        initData.col(strYear)
                );

        Dataset<Row> yearAvTemp =
                yearGroup
                        .mean(strAverageTemperature)
                        .toDF(strYear, strAverageTemperatureForYear)
                        .dropDuplicates(strYear);
        Dataset<Row> yearMinTemp =
                yearGroup
                        .min(strAverageTemperature)
                        .toDF(strYear, strMinTemperatureForYear);
        Dataset<Row> yearMaxTemp =
                yearGroup
                        .max(strAverageTemperature)
                        .toDF(strYear, strMaxTemperatureForYear);

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
                        yearData.col(strDecade)
                );

        Dataset<Row> decadeAvTemp =
                decadeGroup
                        .mean(strAverageTemperatureForYear)
                        .toDF(strDecade, strAverageTemperatureForDecade);
        Dataset<Row> decadeMinTemp =
                decadeGroup
                        .min(strAverageTemperatureForYear)
                        .toDF(strDecade, strMinTemperatureForDecade);
        Dataset<Row> decadeMaxTemp =
                decadeGroup
                        .max(strAverageTemperatureForYear)
                        .toDF(strDecade, strMaxTemperatureForDecade);

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
                        decadeData.col(strCentury)
                );

        Dataset<Row> centuryAvTemp =
                centuryGroup
                        .mean(strAverageTemperatureForDecade)
                        .toDF(strCentury, strAverageTemperatureForCentury);
        Dataset<Row> centuryMinTemp =
                centuryGroup
                        .min(strAverageTemperatureForDecade)
                        .toDF(strCentury, strMinTemperatureForCentury);
        Dataset<Row> centuryMaxTemp =
                centuryGroup
                        .max(strAverageTemperatureForDecade)
                        .toDF(strCentury, strMaxTemperatureForCentury);

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

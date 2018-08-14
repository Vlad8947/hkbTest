package goncharov.hkbTest.handler;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.mutable.ArraySeq;

public class GlobalTemperatureHandler extends DataHandler {

    public GlobalTemperatureHandler(Dataset<Row> data) {
        super(data);
    }

    @Override
    protected void setColumnNames() {
        strAverageTemperature = "LandAverageTemperature";

        strAverageTemperatureForYear = "AverageGlobalTemperatureForYear";
        strMinTemperatureForYear = "MinGlobalTemperatureForYear";
        strMaxTemperatureForYear = "MaxGlobalTemperatureForYear";

        strAverageTemperatureForDecade = "AverageGlobalTemperatureForDecade";
        strMinTemperatureForDecade = "MinGlobalTemperatureForDecade";
        strMaxTemperatureForDecade = "MaxGlobalTemperatureForDecade";

        strAverageTemperatureForCentury = "AverageGlobalTemperatureForCentury";
        strMinTemperatureForCentury = "MinGlobalTemperatureForCentury";
        strMaxTemperatureForCentury = "MaxGlobalTemperatureForCentury";
    }

    @Override
    protected void initializationInitData(Dataset<Row> data) {
        initData =
                data.filter(data.col(strAverageTemperature).isNotNull())
                        .select(
                                data.col(strDt),
                                data.col(strAverageTemperature).cast(DataTypes.FloatType)
                        );
    }

    @Override
    protected String[] getDefaultStrColumnArray() {
        return new String[0];
    }

    public void goTest() {
        handleAndGetFinalData().sort(strYear).show();
    }


}

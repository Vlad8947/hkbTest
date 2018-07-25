package goncharov.hkbTest.handler;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class GlobalTempHandler extends DataHandler {

    public GlobalTempHandler(SQLContext sqlContext) {
        super(sqlContext);
        strAverageTemperature = "LandAverageTemperature";

        strAverageTemperatureForYear = "AverageGlobalTemperatureForYear";
        strMinTemperatureForYear = "MinGlobalTemperatureForYear";
        strMaxTemperatureForYear = "MaxGlobalTemperatureForYear";

        strAverageTemperatureForTenYears = "AverageGlobalTemperatureForTenYears";
        strMinTemperatureForTenYears = "MinGlobalTemperatureForTenYears";
        strMaxTemperatureForTenYears = "MaxGlobalTemperatureForTenYears";

        strAverageTemperatureForTenCentury = "AverageGlobalTemperatureForCentury";
        strMinTemperatureForTenCentury = "MinGlobalTemperatureForCentury";
        strMaxTemperatureForTenCentury = "MaxGlobalTemperatureForCentury";

        finalStructType = SchemaHandler.createStructType(strDt, strAverageTemperatureForYear);
    }

    public Dataset<Row> processData(Dataset<Row> initData) {

        initData = SchemaHandler.setSchemaFromCsv(initData);
        initData = initData.filter(initData.col(strAverageTemperature).isNotNull());




        return null;
    }


}

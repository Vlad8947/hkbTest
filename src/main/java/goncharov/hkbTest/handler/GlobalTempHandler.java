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

        strAverageTemperatureForDecade = "AverageGlobalTemperatureForTenYears";
        strMinTemperatureForDecade = "MinGlobalTemperatureForTenYears";
        strMaxTemperatureForDecade = "MaxGlobalTemperatureForTenYears";

        strAverageTemperatureForCentury = "AverageGlobalTemperatureForCentury";
        strMinTemperatureForCentury = "MinGlobalTemperatureForCentury";
        strMaxTemperatureForCentury = "MaxGlobalTemperatureForCentury";

        finalStructType = SchemaHandler.createStructType(strDt, strAverageTemperatureForYear);
    }

    public Dataset<Row> processData(Dataset<Row> initData) {

        initData = SchemaHandler.setSchemaFromCsv(initData);
        initData = initData.filter(initData.col(strAverageTemperature).isNotNull());




        return null;
    }


}

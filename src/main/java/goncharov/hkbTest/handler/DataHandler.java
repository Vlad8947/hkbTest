package goncharov.hkbTest.handler;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.StructType;
import scala.collection.mutable.ArraySeq;

public abstract class DataHandler {

    protected ArraySeq<Column> initColumns;

    protected static final String
            colCity = "City",
            colCountry = "Country",
            colDt = "dt";
    protected String colAverageTemperature;
    // For Year
    protected String
            colAverageTemperatureForYear,
            colMinTemperatureForYear,
            colMaxTemperatureForYear;
    // For TenYears
    protected String
            colAverageTemperatureForTenYears,
            colMinTemperatureForTenYears,
            colMaxTemperatureForTenYears;
    // For Century
    protected String
            colAverageTemperatureForTenCentury,
            colMinTemperatureForTenCentury,
            colMaxTemperatureForTenCentury;

    protected void setInitColumns(String... initColNames) {
        initColumns = new ArraySeq<>(initColNames.length);
        for(int i = 0; i < initColNames.length; i++) {
            initColumns.update(i, new Column(initColNames[i]));
        }
    }
}

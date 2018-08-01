package goncharov.hkbTest.handler;

import org.apache.spark.sql.SQLContext;

public class CountryTempHandler extends DataHandler {

    private static final String[] INIT_COL_NAMES = {
            "dt",
            "Country",
            "AverageTemperature"
    };

    public CountryTempHandler(SQLContext sqlContext) {
        super(sqlContext);
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


}

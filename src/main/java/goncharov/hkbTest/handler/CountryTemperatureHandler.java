package goncharov.hkbTest.handler;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CountryTemperatureHandler extends TemperatureHandler {

    public CountryTemperatureHandler(Dataset<Row> data) {
        super(data);

    }

    @Override
    protected void setColumnNames() {
        strAverageTemperature = "AverageTemperature";

        strAverageTemperatureForYear = "AverageCountryTemperatureForYear";
        strMinTemperatureForYear = "MinCountryTemperatureForYear";
        strMaxTemperatureForYear = "MaxCountryTemperatureForYear";

        strAverageTemperatureForDecade = "AverageCountryTemperatureForDecade";
        strMinTemperatureForDecade = "MinCountryTemperatureForDecade";
        strMaxTemperatureForDecade = "MaxCountryTemperatureForDecade";

        strAverageTemperatureForCentury = "AverageCountryTemperatureForCentury";
        strMinTemperatureForCentury = "MinCountryTemperatureForCentury";
        strMaxTemperatureForCentury = "MaxCountryTemperatureForCentury";
    }

    @Override
    protected String[] getDefaultStrColumnArray() {
        return new String[]{strCountry};
    }

    public void goTest() {
        initData = initData.filter(initData.col(strCountry).like(
                initData.first().<String>getAs(strCountry)
        ));

        initData.sort(strDt).show();

        handleAndGetFinalData().sort(strYear).show();

    }
}

package ru.goncharov.hkbTest.handlers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CountryTemperatureHandler extends AbstractTemperatureHandler {

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
    public String[] getDefaultStrColumnArray() {
        return new String[]{strCountry};
    }

}

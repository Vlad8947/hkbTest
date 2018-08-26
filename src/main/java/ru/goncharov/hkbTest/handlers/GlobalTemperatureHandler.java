package ru.goncharov.hkbTest.handlers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class GlobalTemperatureHandler extends AbstractTemperatureHandler {

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
    public String[] getDefaultStrColumnArray() {
        return new String[0];
    }

}

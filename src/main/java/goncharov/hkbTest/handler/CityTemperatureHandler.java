package goncharov.hkbTest.handler;

import org.apache.spark.sql.*;

public class CityTemperatureHandler extends AbstractTemperatureHandler {


    public CityTemperatureHandler(Dataset<Row> data) {
        super(data);
    }

    @Override
    protected void setColumnNames(){
        strAverageTemperature = "AverageTemperature";

        strAverageTemperatureForYear = "AverageCityTemperatureForYear";
        strMinTemperatureForYear = "MinCityTemperatureForYear";
        strMaxTemperatureForYear = "MaxCityTemperatureForYear";

        strAverageTemperatureForDecade = "AverageCityTemperatureForDecade";
        strMinTemperatureForDecade = "MinCityTemperatureForDecade";
        strMaxTemperatureForDecade = "MaxCityTemperatureForDecade";

        strAverageTemperatureForCentury = "AverageCityTemperatureForCentury";
        strMinTemperatureForCentury = "MinCityTemperatureForCentury";
        strMaxTemperatureForCentury = "MaxCityTemperatureForCentury";
    }

    @Override
    protected String[] getDefaultStrColumnArray() {
        return new String[]{
                strCity, strCountry
        };
    }

}

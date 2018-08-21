package goncharov.hkbTest.handler;

import org.apache.spark.sql.*;

public class CityTemperatureHandler extends TemperatureHandler {


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

    public void goTest() {
        initData =
                initData.filter(initData.col(strCity).like("Antwerp"));

//        initData.show();

        handleAndGetFinalData()
                .sort(strCity, strYear)
                .show()
        ;
        System.out.println(finalData.first().toString());
        System.out.println(finalData.head().toString());
    }

}

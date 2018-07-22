package goncharov.hkbTest.handler;

public class CityTempHandler extends DataHandler {

    private static final String[] INIT_COL_NAMES = {
            "dt",
            "City",
            "Country",
            "AverageTemperature"
    };

    public CityTempHandler() {

        colAverageTemperatureForYear = "AverageCityTemperatureForYear";
        colMinTemperatureForYear = "MinCityTemperatureForYear";
        colMaxTemperatureForYear = "MaxCityTemperatureForYear";

        colAverageTemperatureForTenYears = "AverageCityTemperatureForTenYears";
        colMinTemperatureForTenYears = "MinCityTemperatureForTenYears";
        colMaxTemperatureForTenYears = "MaxCityTemperatureForTenYears";

        colAverageTemperatureForTenCentury = "AverageCityTemperatureForCentury";
        colMinTemperatureForTenCentury = "MinCityTemperatureForCentury";
        colMaxTemperatureForTenCentury = "MaxCityTemperatureForCentury";
    }


}

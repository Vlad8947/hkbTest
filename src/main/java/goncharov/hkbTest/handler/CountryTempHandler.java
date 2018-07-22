package goncharov.hkbTest.handler;

public class CountryTempHandler extends DataHandler {

    private static final String[] INIT_COL_NAMES = {
            "dt",
            "Country",
            "AverageTemperature"
    };

    public CountryTempHandler() {

        colAverageTemperatureForYear = "AverageCountryTemperatureForYear";
        colMinTemperatureForYear = "MinCountryTemperatureForYear";
        colMaxTemperatureForYear = "MaxCountryTemperatureForYear";

        colAverageTemperatureForTenYears = "AverageCountryTemperatureForTenYears";
        colMinTemperatureForTenYears = "MinCountryTemperatureForTenYears";
        colMaxTemperatureForTenYears = "MaxCountryTemperatureForTenYears";

        colAverageTemperatureForTenCentury = "AverageCountryTemperatureForCentury";
        colMinTemperatureForTenCentury = "MinCountryTemperatureForCentury";
        colMaxTemperatureForTenCentury = "MaxCountryTemperatureForCentury";
    }


}

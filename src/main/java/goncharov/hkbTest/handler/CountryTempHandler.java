package goncharov.hkbTest.handler;

public class CountryTempHandler extends DataHandler {

    private static final String[] INIT_COL_NAMES = {
            "dt",
            "Country",
            "AverageTemperature"
    };

    public CountryTempHandler() {

        strAverageTemperatureForYear = "AverageCountryTemperatureForYear";
        strMinTemperatureForYear = "MinCountryTemperatureForYear";
        strMaxTemperatureForYear = "MaxCountryTemperatureForYear";

        strAverageTemperatureForTenYears = "AverageCountryTemperatureForTenYears";
        strMinTemperatureForTenYears = "MinCountryTemperatureForTenYears";
        strMaxTemperatureForTenYears = "MaxCountryTemperatureForTenYears";

        strAverageTemperatureForTenCentury = "AverageCountryTemperatureForCentury";
        strMinTemperatureForTenCentury = "MinCountryTemperatureForCentury";
        strMaxTemperatureForTenCentury = "MaxCountryTemperatureForCentury";
    }


}

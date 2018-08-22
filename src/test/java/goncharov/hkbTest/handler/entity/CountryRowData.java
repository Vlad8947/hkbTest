package goncharov.hkbTest.handler.entity;

public class CountryRowData implements RowDataInterface {

    private String dt,
            AverageTemperature,
            AverageTemperatureUncertainty,
            Country;

    public CountryRowData(String dt, String country) {
        this.dt = dt;
        Country = country;
    }

    public CountryRowData(String dt, String country, String averageTemperature) {
        this.dt = dt;
        AverageTemperature = averageTemperature;
        Country = country;
    }

    @Override
    public String getDt() {
        return dt;
    }

    @Override
    public String getAverageTemperature() {
        return AverageTemperature;
    }

    public String getAverageTemperatureUncertainty() {
        return AverageTemperatureUncertainty;
    }

    public String getCountry() {
        return Country;
    }
}

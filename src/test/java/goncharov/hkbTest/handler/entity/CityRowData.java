package goncharov.hkbTest.handler.entity;

public class CityRowData implements RowDataInterface {

    private String dt,
            AverageTemperature,
            AverageTemperatureUncertainty,
            City,
            Country,
            Latitude,
            Longitude;

    public CityRowData(String dt, String city, String country) {
        this.dt = dt;
        City = city;
        Country = country;
    }

    public CityRowData(String dt, String city, String country, String averageTemperature) {
        this.dt = dt;
        AverageTemperature = averageTemperature;
        City = city;
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

    public String getCity() {
        return City;
    }

    public String getCountry() {
        return Country;
    }

    public String getLatitude() {
        return Latitude;
    }

    public String getLongitude() {
        return Longitude;
    }
}

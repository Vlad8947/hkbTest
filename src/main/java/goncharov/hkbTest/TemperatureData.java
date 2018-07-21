package goncharov.hkbTest;

public class TemperatureData {

    private String city;
    private String country;
    private String dt;
    private long
            /*City, year*/          AverageCityTemperatureForYear, MinCityTemperatureForYear, MaxCityTemperatureForYear,
            /*City, 10years*/       AverageCityTemperatureForTenYears, MinCityTemperatureForTenYears, MaxCityTemperatureForTenYears,
            /*City, century*/       AverageCityTemperatureForCentury, MinCityTemperatureForCentury, MaxCityTemperatureForCentury,

            /*Country, year*/       AverageCountryTemperatureForYear, MinCountryTemperatureForYear, MaxCountryTemperatureForYear,
            /*Country, 10years*/    AverageCountryTemperatureForTenYears, MinCountryTemperatureForTenYears, MaxCountryTemperatureForTenYears,
            /*Country, century*/    AverageCountryTemperatureForCentury, MinCountryTemperatureForCentury, MaxCountryTemperatureForCentury,

            /*Global, year*/        AverageGlobalTemperatureForYear, MinGlobalTemperatureForYear, MaxGlobalTemperatureForYear,
            /*Global, 10years*/     AverageGlobalTemperatureForTenYears, MinGlobalTemperatureForTenYears, MaxGlobalTemperatureForTenYears,
            /*Global, century*/     AverageGlobalTemperatureForCentury, MinGlobalTemperatureForCentury, MaxGlobalTemperatureForCentury;

    public TemperatureData(String city, String country, String dt) {
        this.city = city;
        this.country = country;
        this.dt = dt;
    }

    public String getCity() {
        return city;
    }

    public String getCountry() {
        return country;
    }

    public String getDt() {
        return dt;
    }

    public long getAverageCityTemperatureForYear() {
        return AverageCityTemperatureForYear;
    }

    public void setAverageCityTemperatureForYear(long averageCityTemperatureForYear) {
        AverageCityTemperatureForYear = averageCityTemperatureForYear;
    }

    public long getMinCityTemperatureForYear() {
        return MinCityTemperatureForYear;
    }

    public void setMinCityTemperatureForYear(long minCityTemperatureForYear) {
        MinCityTemperatureForYear = minCityTemperatureForYear;
    }

    public long getMaxCityTemperatureForYear() {
        return MaxCityTemperatureForYear;
    }

    public void setMaxCityTemperatureForYear(long maxCityTemperatureForYear) {
        MaxCityTemperatureForYear = maxCityTemperatureForYear;
    }

    public long getAverageCityTemperatureForTenYears() {
        return AverageCityTemperatureForTenYears;
    }

    public void setAverageCityTemperatureForTenYears(long averageCityTemperatureForTenYears) {
        AverageCityTemperatureForTenYears = averageCityTemperatureForTenYears;
    }

    public long getMinCityTemperatureForTenYears() {
        return MinCityTemperatureForTenYears;
    }

    public void setMinCityTemperatureForTenYears(long minCityTemperatureForTenYears) {
        MinCityTemperatureForTenYears = minCityTemperatureForTenYears;
    }

    public long getMaxCityTemperatureForTenYears() {
        return MaxCityTemperatureForTenYears;
    }

    public void setMaxCityTemperatureForTenYears(long maxCityTemperatureForTenYears) {
        MaxCityTemperatureForTenYears = maxCityTemperatureForTenYears;
    }

    public long getAverageCityTemperatureForCentury() {
        return AverageCityTemperatureForCentury;
    }

    public void setAverageCityTemperatureForCentury(long averageCityTemperatureForCentury) {
        AverageCityTemperatureForCentury = averageCityTemperatureForCentury;
    }

    public long getMinCityTemperatureForCentury() {
        return MinCityTemperatureForCentury;
    }

    public void setMinCityTemperatureForCentury(long minCityTemperatureForCentury) {
        MinCityTemperatureForCentury = minCityTemperatureForCentury;
    }

    public long getMaxCityTemperatureForCentury() {
        return MaxCityTemperatureForCentury;
    }

    public void setMaxCityTemperatureForCentury(long maxCityTemperatureForCentury) {
        MaxCityTemperatureForCentury = maxCityTemperatureForCentury;
    }

    public long getAverageCountryTemperatureForYear() {
        return AverageCountryTemperatureForYear;
    }

    public void setAverageCountryTemperatureForYear(long averageCountryTemperatureForYear) {
        AverageCountryTemperatureForYear = averageCountryTemperatureForYear;
    }

    public long getMinCountryTemperatureForYear() {
        return MinCountryTemperatureForYear;
    }

    public void setMinCountryTemperatureForYear(long minCountryTemperatureForYear) {
        MinCountryTemperatureForYear = minCountryTemperatureForYear;
    }

    public long getMaxCountryTemperatureForYear() {
        return MaxCountryTemperatureForYear;
    }

    public void setMaxCountryTemperatureForYear(long maxCountryTemperatureForYear) {
        MaxCountryTemperatureForYear = maxCountryTemperatureForYear;
    }

    public long getAverageCountryTemperatureForTenYears() {
        return AverageCountryTemperatureForTenYears;
    }

    public void setAverageCountryTemperatureForTenYears(long averageCountryTemperatureForTenYears) {
        AverageCountryTemperatureForTenYears = averageCountryTemperatureForTenYears;
    }

    public long getMinCountryTemperatureForTenYears() {
        return MinCountryTemperatureForTenYears;
    }

    public void setMinCountryTemperatureForTenYears(long minCountryTemperatureForTenYears) {
        MinCountryTemperatureForTenYears = minCountryTemperatureForTenYears;
    }

    public long getMaxCountryTemperatureForTenYears() {
        return MaxCountryTemperatureForTenYears;
    }

    public void setMaxCountryTemperatureForTenYears(long maxCountryTemperatureForTenYears) {
        MaxCountryTemperatureForTenYears = maxCountryTemperatureForTenYears;
    }

    public long getAverageCountryTemperatureForCentury() {
        return AverageCountryTemperatureForCentury;
    }

    public void setAverageCountryTemperatureForCentury(long averageCountryTemperatureForCentury) {
        AverageCountryTemperatureForCentury = averageCountryTemperatureForCentury;
    }

    public long getMinCountryTemperatureForCentury() {
        return MinCountryTemperatureForCentury;
    }

    public void setMinCountryTemperatureForCentury(long minCountryTemperatureForCentury) {
        MinCountryTemperatureForCentury = minCountryTemperatureForCentury;
    }

    public long getMaxCountryTemperatureForCentury() {
        return MaxCountryTemperatureForCentury;
    }

    public void setMaxCountryTemperatureForCentury(long maxCountryTemperatureForCentury) {
        MaxCountryTemperatureForCentury = maxCountryTemperatureForCentury;
    }

    public long getAverageGlobalTemperatureForYear() {
        return AverageGlobalTemperatureForYear;
    }

    public void setAverageGlobalTemperatureForYear(long averageGlobalTemperatureForYear) {
        AverageGlobalTemperatureForYear = averageGlobalTemperatureForYear;
    }

    public long getMinGlobalTemperatureForYear() {
        return MinGlobalTemperatureForYear;
    }

    public void setMinGlobalTemperatureForYear(long minGlobalTemperatureForYear) {
        MinGlobalTemperatureForYear = minGlobalTemperatureForYear;
    }

    public long getMaxGlobalTemperatureForYear() {
        return MaxGlobalTemperatureForYear;
    }

    public void setMaxGlobalTemperatureForYear(long maxGlobalTemperatureForYear) {
        MaxGlobalTemperatureForYear = maxGlobalTemperatureForYear;
    }

    public long getAverageGlobalTemperatureForTenYears() {
        return AverageGlobalTemperatureForTenYears;
    }

    public void setAverageGlobalTemperatureForTenYears(long averageGlobalTemperatureForTenYears) {
        AverageGlobalTemperatureForTenYears = averageGlobalTemperatureForTenYears;
    }

    public long getMinGlobalTemperatureForTenYears() {
        return MinGlobalTemperatureForTenYears;
    }

    public void setMinGlobalTemperatureForTenYears(long minGlobalTemperatureForTenYears) {
        MinGlobalTemperatureForTenYears = minGlobalTemperatureForTenYears;
    }

    public long getMaxGlobalTemperatureForTenYears() {
        return MaxGlobalTemperatureForTenYears;
    }

    public void setMaxGlobalTemperatureForTenYears(long maxGlobalTemperatureForTenYears) {
        MaxGlobalTemperatureForTenYears = maxGlobalTemperatureForTenYears;
    }

    public long getAverageGlobalTemperatureForCentury() {
        return AverageGlobalTemperatureForCentury;
    }

    public void setAverageGlobalTemperatureForCentury(long averageGlobalTemperatureForCentury) {
        AverageGlobalTemperatureForCentury = averageGlobalTemperatureForCentury;
    }

    public long getMinGlobalTemperatureForCentury() {
        return MinGlobalTemperatureForCentury;
    }

    public void setMinGlobalTemperatureForCentury(long minGlobalTemperatureForCentury) {
        MinGlobalTemperatureForCentury = minGlobalTemperatureForCentury;
    }

    public long getMaxGlobalTemperatureForCentury() {
        return MaxGlobalTemperatureForCentury;
    }

    public void setMaxGlobalTemperatureForCentury(long maxGlobalTemperatureForCentury) {
        MaxGlobalTemperatureForCentury = maxGlobalTemperatureForCentury;
    }
}

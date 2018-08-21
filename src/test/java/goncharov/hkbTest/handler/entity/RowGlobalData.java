package goncharov.hkbTest.handler.entity;

import java.io.Serializable;

public class RowGlobalData implements RowDataInterface, Serializable {

    private String dt,LandAverageTemperature,LandAverageTemperatureUncertainty,LandMaxTemperature,LandMaxTemperatureUncertainty,LandMinTemperature,LandMinTemperatureUncertainty,LandAndOceanAverageTemperature,LandAndOceanAverageTemperatureUncertainty;

    public RowGlobalData(String dt) {
        this.dt = dt;
    }

    public void setLandAverageTemperature(String landAverageTemperature) {
        LandAverageTemperature = landAverageTemperature;
    }

    @Override
    public String getDt() {
        return dt;
    }

    @Override
    public String getAverageTemper() {
        return getLandAverageTemperature();
    }

    public String getLandAverageTemperature() {
        return LandAverageTemperature;
    }

    public String getLandAverageTemperatureUncertainty() {
        return LandAverageTemperatureUncertainty;
    }

    public String getLandMaxTemperature() {
        return LandMaxTemperature;
    }

    public String getLandMaxTemperatureUncertainty() {
        return LandMaxTemperatureUncertainty;
    }

    public String getLandMinTemperature() {
        return LandMinTemperature;
    }

    public String getLandMinTemperatureUncertainty() {
        return LandMinTemperatureUncertainty;
    }

    public String getLandAndOceanAverageTemperature() {
        return LandAndOceanAverageTemperature;
    }

    public String getLandAndOceanAverageTemperatureUncertainty() {
        return LandAndOceanAverageTemperatureUncertainty;
    }
}

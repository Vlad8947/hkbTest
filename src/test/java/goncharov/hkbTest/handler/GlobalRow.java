package goncharov.hkbTest.handler;

import java.awt.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class GlobalRow {

    private String dt,LandAverageTemperature,LandAverageTemperatureUncertainty,LandMaxTemperature,LandMaxTemperatureUncertainty,LandMinTemperature,LandMinTemperatureUncertainty,LandAndOceanAverageTemperature,LandAndOceanAverageTemperatureUncertainty;

    public GlobalRow(String dt) {
        this.dt = dt;
    }

    public void setLandAverageTemperature(String landAverageTemperature) {
        LandAverageTemperature = landAverageTemperature;
    }

    public String getDt() {
        return dt;
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

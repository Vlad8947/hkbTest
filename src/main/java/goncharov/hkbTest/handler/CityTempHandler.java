package goncharov.hkbTest.handler;

import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.List;

public class CityTempHandler extends DataHandler {

    private static Dataset<Row> initData;
    private static List<Dataset<Row>> citiesData;

    public CityTempHandler() {
        strAverageTemperature = "AverageTemperature";

        strAverageTemperatureForYear = "AverageCityTemperatureForYear";
        strMinTemperatureForYear = "MinCityTemperatureForYear";
        strMaxTemperatureForYear = "MaxCityTemperatureForYear";

        strAverageTemperatureForTenYears = "AverageCityTemperatureForTenYears";
        strMinTemperatureForTenYears = "MinCityTemperatureForTenYears";
        strMaxTemperatureForTenYears = "MaxCityTemperatureForTenYears";

        strAverageTemperatureForTenCentury = "AverageCityTemperatureForCentury";
        strMinTemperatureForTenCentury = "MinCityTemperatureForCentury";
        strMaxTemperatureForTenCentury = "MaxCityTemperatureForCentury";
    }

    public Dataset<Row> process(Dataset<Row> data) {
        initData = SchemaHandler.setSchemaFromCsv(data);
        initData = initData.filter(initCol(strAverageTemperature).isNotNull())
                .select(strDt, strCity, strCountry, strAverageTemperature);
        initData.persist(StorageLevel.MEMORY_ONLY());

        Dataset<String> cities = initData.select(initCol(strCity)).dropDuplicates().map(
                row -> row.getString(0),
                Encoders.STRING()
        );

        citiesData = new ArrayList<>();
        cities.foreach(city -> {
            citiesData.add(
                    initData.filter(row -> row.<String>getAs(strCity).equals(city))
            );
        });

        citiesData.get(0).show();
        return null;
    }

    private Column initCol(String name) {
        return initData.col(name);
    }

}

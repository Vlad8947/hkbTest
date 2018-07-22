package goncharov.hkbTest.handler;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public class GlobalTempHandler extends DataHandler {

    public GlobalTempHandler() {

        colAverageTemperature = "LandAverageTemperature";

        colAverageTemperatureForYear = "AverageGlobalTemperatureForYear";
        colMinTemperatureForYear = "MinGlobalTemperatureForYear";
        colMaxTemperatureForYear = "MaxGlobalTemperatureForYear";

        colAverageTemperatureForTenYears = "AverageGlobalTemperatureForTenYears";
        colMinTemperatureForTenYears = "MinGlobalTemperatureForTenYears";
        colMaxTemperatureForTenYears = "MaxGlobalTemperatureForTenYears";

        colAverageTemperatureForTenCentury = "AverageGlobalTemperatureForCentury";
        colMinTemperatureForTenCentury = "MinGlobalTemperatureForCentury";
        colMaxTemperatureForTenCentury = "MaxGlobalTemperatureForCentury";
    }

    public Dataset<Row> handleData(Dataset<Row> initData) {

        initData = SchemaHandler.setSchemaFromCsv(initData)
                .filter(initData.col(colAverageTemperature).isNotNull())
                .sort(colDt);

        List<Row> list = handleDataForYear(initData);


        Dataset<Row> finalData;

        return null;
    }

    private List<Row> handleDataForYear(Dataset<Row> initData){

        ArrayList<Row> list = new ArrayList<>();
        Row first = initData.first();
        final StructType structType = SchemaHandler.createStructType(colDt, colAverageTemperatureForYear);

        final int dtField = first.fieldIndex(colDt);
        final int temperField = first.fieldIndex(colAverageTemperature);
        int firstYear = Integer.parseInt(first.getString(dtField).split("-")[0]);

        AtomicInteger initYear = new AtomicInteger(firstYear);
        AtomicInteger monthSum = new AtomicInteger(0);
        AtomicLong tempSumm = new AtomicLong(0);

        initData.foreach(row -> {

            int year = Integer.parseInt(
                    row.getString(dtField).split("-")[0]
            );

            if(year != initYear.get()){

                String[] values = {
                        initYear.toString(),
                        Long.toString((tempSumm.get() / monthSum.get()))
                };
                list.add(new GenericRowWithSchema(values, structType));

                initYear.set(year);
                tempSumm.set(0);
                monthSum.set(0);
            }
            else {

            }
        });

        return list;
    }

}

package goncharov.hkbTest.handler;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.mutable.ArraySeq;

import java.util.ArrayList;
import java.util.List;

public class SchemaHandler {

    public SchemaHandler() {
    }

    public static Dataset<Row> setSchemaFromCsv(Dataset<Row> dataset) {

        Row header = dataset.first();
        ArraySeq<String> schema = new ArraySeq<>(header.length());

        for (int i = 0; i < header.length(); i++) {
            schema.update(i, header.getString(i));
        }

        dataset = dataset.toDF(schema);
        dataset = dataset.filter(row -> {
            return !(row.equals(header));
        });

        return dataset;
    }

    public static StructType createStructType(String... colNames){

        List<StructField> fieldList = new ArrayList<>();
        for(String col: colNames){
            fieldList.add(
                    DataTypes.createStructField(col, DataTypes.StringType, true)
            );
        }
        return DataTypes.createStructType(fieldList);
    }
}

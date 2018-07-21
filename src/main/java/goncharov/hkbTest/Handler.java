package goncharov.hkbTest;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.mutable.ArraySeq;

import java.util.List;

public class Handler {

    public Handler() {
    }

    public Dataset<Row> setSchema(Dataset<Row> dataset) {

        Row header = dataset.first();
        ArraySeq<String> schema = new ArraySeq<>(header.length());

        for (int i = 0; i < header.length(); i++) {
            schema.update(i, header.getString(i));
        }

        dataset = dataset.toDF(schema);
        return dataset.filter(row -> !(row.equals(header)));
    }
}

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class ReadFromDBPipeline {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);


        // read from text file
        PCollection<KV<String, String>> rows = pipeline.apply(JdbcIO.<KV<String, String>>read()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                                .create("org.postgresql.Driver","jdbc:postgresql://localhost:5434/company_service")
                               //.create("org.postgresql.Driver","jdbc:postgresql://postgres@postgresql:5434/company_service")
                        .withUsername("postgres")
                        .withPassword("postgres"))
                .withQuery("select id, matricule from driver")
                .withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                .withRowMapper(new JdbcIO.RowMapper<KV<String, String>>() {
                    @Override
                    public KV<String, String> mapRow(ResultSet resultSet) throws Exception {
                        return KV.of(resultSet.getString(1), resultSet.getString(2));
                    }
                })
        );

        rows.apply(MapElements.via(new SimpleFunction<KV<String, String>, String>() {
            @Override
            public String apply(KV<String, String> input) {
                return String.format("%s => %s", input.getKey(), input.getValue());
            }
        }))
        .apply(TextIO.write().to("drivers_registration_number."))
        ;

        pipeline.run().waitUntilFinish();
    }
}

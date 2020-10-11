import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class DBtoDBPipeline {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        // Company Service DB
        JdbcIO.DataSourceConfiguration dataSource = getConnection(
                "jdbc:postgresql://localhost:5434/company_service",
                "postgres",
                "postgres");

        // 2Cloud DB
        JdbcIO.DataSourceConfiguration dataDestination = getConnection(
                "jdbc:postgresql://localhost:5432/postgres",
                "www-data",
                "ubitransport");

        // Read From 2Cloud BD
        PCollection<KV<String, String>> rows = pipeline.apply(JdbcIO.<KV<String, String>>read()
                .withDataSourceConfiguration(dataSource)
                .withQuery("select id, matricule from driver where matricule is not null")
                .withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                .withRowMapper(new JdbcIO.RowMapper<KV<String, String>>() {
                    @Override
                    public KV<String, String> mapRow(ResultSet resultSet) throws Exception {
                        return KV.of(resultSet.getString(1), resultSet.getString(2));
                    }
                })
        );

        // Write to Company Service DB
        rows.apply(JdbcIO.<KV<String, String>>write()
        .withDataSourceConfiguration(dataDestination)
        .withStatement(String.format("insert into %s values(?, ?)", "driver_pipeline"))
        .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<KV<String, String>>() {
            @Override
            public void setParameters(KV<String, String> element, PreparedStatement query) throws Exception {
                query.setString(1, element.getKey());
                query.setString(2, element.getValue());
                System.out.printf("QUERY BEING EXECUTED ===> [ %s ]%n", query);
            }
        }));

        pipeline.run().waitUntilFinish();
    }

    private static JdbcIO.DataSourceConfiguration getConnection(String url, String username, String pwd) {
        return JdbcIO.DataSourceConfiguration
                .create("org.postgresql.Driver", url)
                .withUsername(username)
                .withPassword(pwd);
    }
}

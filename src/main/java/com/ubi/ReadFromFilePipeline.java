package com.ubi;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;

import java.sql.ResultSet;

public class ReadFromFilePipeline {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        // Company Service DB
        JdbcIO.DataSourceConfiguration dataSource = getConnection();

        HSSFWorkbook workbook = new HSSFWorkbook();
        HSSFSheet sheet = workbook.createSheet("Employees sheet");
        int rownum = 0;
        Cell cell;
        Row row;
        HSSFCellStyle style = createStyleForTitle(workbook);
        row = sheet.createRow(rownum);

        // Id
        cell = row.createCell(0, CellType.STRING);
        cell.setCellValue("Id");
        cell.setCellStyle(style);
        // Name
        cell = row.createCell(1, CellType.STRING);
        cell.setCellValue("Name");
        cell.setCellStyle(style);


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

        rows.apply(ParDo.of(
            new DoFn<KV<String, String>, KV<String, String>>() {
                @ProcessElement
                public void process(ProcessContext c) {
                    System.out.println("Key = " + c.element().getKey() + ", value = " + c.element().getValue());
                }
            }
        ));
        
        pipeline.run().waitUntilFinish();
    }

    private static HSSFCellStyle createStyleForTitle(HSSFWorkbook workbook) {
        HSSFFont font = workbook.createFont();
        font.setBold(true);
        HSSFCellStyle style = workbook.createCellStyle();
        style.setFont(font);
        return style;
    }

    private static JdbcIO.DataSourceConfiguration getConnection() {
        return JdbcIO.DataSourceConfiguration
                .create("org.postgresql.Driver", "jdbc:postgresql://localhost:5434/company_service")
                .withUsername("postgres")
                .withPassword("postgres");
    }
}

package backendScripts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import javax.validation.constraints.NotNull;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public class GenerateSql {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        try {
            if (true) {
                generateEconomicIndustrySqlFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void generateEconomicIndustrySqlFile() throws IOException {
        Workbook workbook = WorkbookFactory.create(new FileInputStream("misc/细分行业集聚度.xlsx"));
        Sheet sheet = workbook.getSheet("两位数行业集聚度");
        Writer writer = new FileWriter("bin/import_industry_concentration.sql");
        /* create partition */
        String sql = "create table sd_entity_data_industry_concentration partition of sd_entity_data for values in ('industry_concentration');";
        writeAndPrint(writer, sql);
        sql = "create index on sd_entity_data_industry_concentration (label);";
        writeAndPrint(writer, sql);
        /* get province and city */
        Row provinceRow = sheet.getRow(0);
        Row cityRow = sheet.getRow(1);
        /* loop and build */
        for (int i = 2; i <= sheet.getLastRowNum(); i += 1) {
            Row row = sheet.getRow(i);
            for (int j = 2; j < Math.min(provinceRow.getLastCellNum(), cityRow.getLastCellNum()); j += 1) {
                ObjectNode data = objectMapper.createObjectNode();
                data.put("industry_code", row.getCell(0).toString());
                data.put("industry_name", row.getCell(1).toString());
                data.put("province", provinceRow.getCell(j).toString());
                data.put("city", cityRow.getCell(j).toString());
                data.put("concentration", row.getCell(j).toString());
                data.put("stat_time_year", "2017");
                sql = buildStandardSql("industry_concentration", row.getCell(1).toString(), provinceRow.getCell(j).toString(), cityRow.getCell(j).toString(), null, null, "industry", null, data.toString());
                writeAndPrint(writer, sql);
            }
        }
        writer.flush();
    }

    private static void writeAndPrint(Writer writer, String sql) throws IOException {
        writer.write(sql + "\n");
        if (false) {
            System.out.println(sql);
        }
    }

    private static String buildStandardSql(@NotNull String label, @NotNull String entityName, String province, String city, String lonDecimal, String latDecimal, String category, String geom, String dataJson) {
        province = province == null ? province : "'" + province + "'";
        city = city == null ? city : "'" + city + "'";
        category = category == null ? category : "'" + category + "'";
        geom = geom == null ? geom : "'" + geom + "'";
        dataJson = dataJson == null ? dataJson : "'" + dataJson + "'";
        return "insert into sd_entity_data (label, create_time, entity_name, province, city, lon, lat, category, geom, data, is_valid) " +
                "values ('" + label + "', 'now', '" + entityName + "', " + province + ", " + city + ", " + lonDecimal + ", " + latDecimal + ", " + category + ", " + geom + ", " + dataJson + ", true);";
    }
}

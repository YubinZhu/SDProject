package backendScripts;

import app.service.AMapService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import javax.validation.constraints.NotNull;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Optional;

public class GenerateSql {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        try {
            if (false) {
                generateIndustryConcentrationSqlFile();
            }
            if (false) {
                generateChinaTop500SqlFile();
            }
            if (false) {
                generateListedCompanySqlFile();
            }
            if (true) {
                generateNationalCompanyTechCenterSqlFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void generateIndustryConcentrationSqlFile() throws IOException {
        Workbook workbook = WorkbookFactory.create(new FileInputStream("misc/细分行业集聚度.xlsx"));
        Sheet sheet = workbook.getSheet("两位数行业集聚度");
        Writer writer = new FileWriter("bin/import_industry_concentration.sql");
        /* create partition */
        String sql = "drop table if exists sd_entity_data_industry_concentration;";
        writeAndPrint(writer, sql);
        sql = "create table sd_entity_data_industry_concentration partition of sd_entity_data for values in ('industry_concentration');";
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
                data.put("stat_time_year", "2017");
                data.put("industry_code", row.getCell(0).toString());
                data.put("industry_name", row.getCell(1).toString());
                data.put("province", provinceRow.getCell(j).toString());
                data.put("city", cityRow.getCell(j).toString());
                data.put("concentration", row.getCell(j).toString());
                sql = buildStandardSql("industry_concentration", row.getCell(1).toString(), provinceRow.getCell(j).toString(), cityRow.getCell(j).toString(), null, null, "industry", null, data.toString());
                writeAndPrint(writer, sql);
            }
        }
        writer.flush();
    }

    private static void generateChinaTop500SqlFile() throws IOException {
        Workbook workbook = WorkbookFactory.create(new FileInputStream("misc/2018年中国500强排行榜.xlsx"));
        Sheet sheet = workbook.getSheetAt(0);
        Writer writer = new FileWriter("bin/import_china_top_500.sql");
        /* create partition */
        String sql = "drop table if exists sd_entity_data_china_top_500;";
        writeAndPrint(writer, sql);
        sql = "create table sd_entity_data_china_top_500 partition of sd_entity_data for values in ('china_top_500');";
        writeAndPrint(writer, sql);
        sql = "create index on sd_entity_data_china_top_500 (label);";
        writeAndPrint(writer, sql);

        /* loop and build */
        for (int i = 1; i <= sheet.getLastRowNum(); i += 1) {
            Row row = sheet.getRow(i);
            ObjectNode data = objectMapper.createObjectNode();
            data.put("stat_time_year", "2018");
            data.put("rank", row.getCell(0).toString());
            data.put("rank_2017", row.getCell(1).toString());
            sql = buildStandardSql("china_top_500", row.getCell(2).toString(), null, null, null, null, null, null, data.toString());
            writeAndPrint(writer, sql);
        }
        writer.flush();
    }

    private static void generateListedCompanySqlFile() throws IOException {
        Workbook workbook = WorkbookFactory.create(new FileInputStream("misc/上市_全部产业.xlsx"));
        Sheet sheet = workbook.getSheetAt(0);
        Writer writer = new FileWriter("bin/import_listed_company.sql");
        /* create partition */
        String sql = "drop table if exists sd_entity_data_import_listed_company;";
        writeAndPrint(writer, sql);
        sql = "create table sd_entity_data_import_listed_company partition of sd_entity_data for values in ('listed_company');";
        writeAndPrint(writer, sql);
        sql = "create index on sd_entity_data_import_listed_company (label);";
        writeAndPrint(writer, sql);

        /* loop and build */
        for (int i = 1; i <= sheet.getLastRowNum(); i += 1) {
            Row row = sheet.getRow(i);
            ObjectNode data = objectMapper.createObjectNode();
            data.put("stat_time_year", "2020");
            data.put("stock_code", row.getCell(0).toString());
            data.put("stock_abbreviation", row.getCell(1).toString());
            data.put("unified_social_credit_code", row.getCell(2).toString());
            data.put("company_name", row.getCell(3).toString());
            data.put("province", row.getCell(4).toString());
            data.put("CCID_industry", Optional.ofNullable(row.getCell(5)).map(Cell::toString).orElse(null));
            data.put("CCID_first", Optional.ofNullable(row.getCell(6)).map(Cell::toString).orElse(null));
            data.put("CCID_second", Optional.ofNullable(row.getCell(7)).map(Cell::toString).orElse(null));
            data.put("CCID_third", Optional.ofNullable(row.getCell(8)).map(Cell::toString).orElse(null));
            data.put("CCID_fourth", Optional.ofNullable(row.getCell(9)).map(Cell::toString).orElse(null));
            data.put("CCID_fifth", Optional.ofNullable(row.getCell(10)).map(Cell::toString).orElse(null));
            data.put("address", row.getCell(11).toString());
            data.put("profile", row.getCell(12).toString());
            data.put("main_product_name", row.getCell(13).toString());
            data.put("main_product_type", row.getCell(14).toString());
            data.put("power_consumption_2017", row.getCell(15).toString());
            data.put("power_consumption_2018", row.getCell(16).toString());
            data.put("power_consumption_2019", row.getCell(17).toString());
            data.put("waste_water_discharge_2017", row.getCell(18).toString());
            data.put("waste_water_discharge_2018", row.getCell(19).toString());
            data.put("waste_water_discharge_2019", row.getCell(20).toString());
            data.put("operating_income_2017", row.getCell(21).toString());
            data.put("operating_income_2018", row.getCell(22).toString());
            data.put("operating_income_2019", row.getCell(23).toString());
            data.put("operating_cost_2017", row.getCell(24).toString());
            data.put("operating_cost_2018", row.getCell(25).toString());
            data.put("operating_cost_2019", row.getCell(26).toString());
            data.put("employee_number_2017", row.getCell(27).toString());
            data.put("employee_number_2018", row.getCell(28).toString());
            data.put("employee_number_2019", row.getCell(29).toString());
            data.put("research_expenditure_2017", row.getCell(30).toString());
            data.put("research_expenditure_2018", row.getCell(31).toString());
            data.put("research_expenditure_2019", row.getCell(32).toString());
            data.put("taxes_and_surcharges_2017", row.getCell(33).toString());
            data.put("taxes_and_surcharges_2018", row.getCell(34).toString());
            data.put("taxes_and_surcharges_2019", row.getCell(35).toString());
            data.put("operating_profit", row.getCell(36).toString());
            String location = AMapService.getGeo(data.get("address").asText());
            if (location != null) {
                data.put("lon", location.split(",")[0]);
                data.put("lat", location.split(",")[1]);
                sql = buildStandardSql("listed_company",
                        data.get("company_name").asText(),
                        data.get("province").asText(),
                        null,
                        data.get("lon").asText(),
                        data.get("lat").asText(),
                        "company",
                        null,
                        data.toString());
            } else {
                sql = buildStandardSql("listed_company",
                        data.get("company_name").asText(),
                        data.get("province").asText(),
                        null,
                        null,
                        null,
                        data.get("province").asText(),
                        null,
                        data.toString());
            }

            writeAndPrint(writer, sql);
        }
        writer.flush();
    }

    private static void generateNationalCompanyTechCenterSqlFile() throws IOException {
        Workbook workbook = WorkbookFactory.create(new FileInputStream("misc/国家企业技术中心_2019.xlsx"));
        Sheet sheet = workbook.getSheetAt(0);
        Writer writer = new FileWriter("bin/import_national_company_tech_center.sql");
        /* create partition */
        String sql = "drop table if exists sd_entity_data_national_company_tech_center;";
        writeAndPrint(writer, sql);
        sql = "create table sd_entity_data_national_company_tech_center partition of sd_entity_data for values in ('tech_center');";
        writeAndPrint(writer, sql);
        sql = "create index on sd_entity_data_national_company_tech_center (label);";
        writeAndPrint(writer, sql);

        /* loop and build */
        for (int i = 1; i <= sheet.getLastRowNum(); i += 1) {
            Row row = sheet.getRow(i);
            ObjectNode data = objectMapper.createObjectNode();
            data.put("tech_center_name", row.getCell(1).toString());
            data.put("company_name", row.getCell(2).toString());
            data.put("stat_time_year", row.getCell(3).toString());
            data.put("publish_institute", row.getCell(4).toString());
            data.put("level", row.getCell(5).toString());
            data.put("category", row.getCell(6).toString());
            data.put("sub_category", Optional.ofNullable(row.getCell(7)).map(Cell::toString).orElse(null));
            data.put("CCID_industry", Optional.ofNullable(row.getCell(8)).map(Cell::toString).orElse(null));
            data.put("province", row.getCell(9).toString());
            data.put("address", Optional.ofNullable(row.getCell(10)).map(Cell::toString).orElse(null));
            String location = AMapService.getGeo(data.get("address").asText());
            if (location != null) {
                data.put("lon", location.split(",")[0]);
                data.put("lat", location.split(",")[1]);
                sql = buildStandardSql("tech_center",
                        data.get("tech_center_name").asText(),
                        data.get("province").asText(),
                        null,
                        data.get("lon").asText(),
                        data.get("lat").asText(),
                        "institution",
                        null,
                        data.toString());
            } else {
                sql = buildStandardSql("tech_center",
                        data.get("tech_center_name").asText(),
                        data.get("province").asText(),
                        null,
                        null,
                        null,
                        "institution",
                        null,
                        data.toString());
            }

            writeAndPrint(writer, sql);
        }
        writer.flush();
    }

    private static void writeAndPrint(Writer writer, String sql) throws IOException {
        if (true) {
            writer.write(sql + "\n");
        }
        if (true) {
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

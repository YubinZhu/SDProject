package backendScripts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.*;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by yubzhu on 2019/7/7
 */

public class ImportData {

    private static final String address = "https://restapi.amap.com/v3/geocode/geo";

    private static final String key = "c346ef3fe374bf57803d4eb57aca0fb0";

    private static final String backupKey = "f4edf4d440e4de85a51cb04a37586532";

    private static final boolean batch = false;

    private static final int maxThreads = 16;

    /* WARNING: If main() fucntion don't stop, there must be some wrong data. */
    public static void main(String[] args) {
        try {
            /* listed company */
//            createListedCompanyTable();
//            importListedCompanyData();
//            addListedCompanyColumn();
//            updateListedCompanyProvinceCity();
//            /* shandong company */
            createShandongCompanyTable();
            importShandongCompanyData();
            updateShandongCompanyProvince();
//            /* shandong digital park */
//            createShandongDigitalParkTable();
//            importShandongDigitalParkData();
//            /* shandong digital company */
//            createShandongDigitalCompanyTable();
//            importShandongDigitalCompanyData();
//            /* district boundary */
//            createDistrictBoundaryTable();
//            importDistrictBoundaryData();
//            /* hebei cluster */
//            createHebeiClusterTable();
//            importHebeiClusterData();
//            /* five hundred */
//            createFiveHundredTable();
//            importFiveHundredTable();
//            /* custom region */
//            createCustomRegionTable();
        } catch (ClassNotFoundException | SQLException | IOException | NullPointerException e) {
            e.printStackTrace();
        }
        System.out.flush();
    }

    private static void createListedCompanyTable() throws ClassNotFoundException, SQLException {
        String sqlSentence;
        sqlSentence = "drop table if exists listed_company";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
        /* WARNING: Don't import production description 'cause string format is awful and too long. */
        sqlSentence = "create table listed_company(id serial primary key, sec_code varchar(16), sec_abbr_name varchar(16), com_chn_name varchar(32), " +
                "province varchar(16), website varchar(64), address varchar(128), employee_num int, city varchar(16), " +
                "income_2018 float4, income_2017 float4, income_2016 float4, income_2015 float4, income_2014 float4, " +
                "income_rate_2018 float4, income_rate_2017 float4, income_rate_2016 float4, income_rate_2015 float4, income_rate_2014 float4, " +
                "rsh_cost_2018 float4, rsh_cost_2017 float4, rsh_cost_2016 float4, rsh_cost_2015 float4, rsh_cost_2014 float4, " +
                "rsh_cost_ratio_2018 float4, rsh_cost_ratio_2017 float4, rsh_cost_ratio_2016 float4, rsh_cost_ratio_2015 float4, rsh_cost_ratio_2014 float4, " +
                "gov_sub_2018 float4, gov_sub_2017 float4, gov_sub_2016 float4, gov_sub_2015 float4, gov_sub_2014 float4, " +
                "tax_2018 float4, tax_2017 float4, tax_2016 float4, tax_2015 float4, tax_2014 float4, " +
                "debt_ratio_2018 float4, debt_ratio_2017 float4, debt_ratio_2016 float4, debt_ratio_2015 float4, debt_ratio_2014 float4, " +
                "profit_2018 float4, profit_2017 float4, profit_2016 float4, profit_2015 float4, profit_2014 float4, " +
                "to_ratio_2018 float4, to_ratio_2017 float4, to_ratio_2016 float4, to_ratio_2015 float4, to_ratio_2014 float4, " +
                "total_value bigint, remission_2018 float4, remission_2017 float4, remission_2016 float4, remission_2015 float4, remission_2014 float4, " +
                "board_type varchar(8), bachelor_num int, master_num int, doctor_num int, lon float4, lat float4)";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
    }

    private static void importListedCompanyData() throws IOException {
        Workbook workbook = WorkbookFactory.create(new FileInputStream("misc/全部A股+科创板+新三板.xlsx"));
        for (int i = 0; i < workbook.getNumberOfSheets(); i += 1) {
            Sheet sheet = workbook.getSheetAt(i);
            for (int j = 1; j <= sheet.getLastRowNum(); j += 1) {
                Row row = sheet.getRow(j);
                String location;
                try {
                    URL url = new URL(address + "?address=" + row.getCell(5).toString().replace("#", "号").replace(" ", "") + "&key=" + key + "&batch=" + batch);
                    location = new ObjectMapper().readValue(url, ObjectNode.class).get("geocodes").get(0).get("location").asText();
                } catch (NullPointerException e) {
                    System.out.println("#listed-" + i + "-" + j + ": get location failed.");
                    continue;
                } catch (IOException e) {
                    System.out.println("#listed-" + i + "-" + j + ": unknown error.");
                    continue;
                }
                String sqlSentence = "insert into listed_company(sec_code, sec_abbr_name, com_chn_name, " +
                        "province, website, address, employee_num, city, " +
                        "income_2018, income_2017, income_2016, income_2015, income_2014, " +
                        "income_rate_2018, income_rate_2017, income_rate_2016, income_rate_2015, income_rate_2014, " +
                        "rsh_cost_2018, rsh_cost_2017, rsh_cost_2016, rsh_cost_2015, rsh_cost_2014, " +
                        "rsh_cost_ratio_2018, rsh_cost_ratio_2017, rsh_cost_ratio_2016, rsh_cost_ratio_2015, rsh_cost_ratio_2014, " +
                        "gov_sub_2018, gov_sub_2017, gov_sub_2016, gov_sub_2015, gov_sub_2014, " +
                        "tax_2018, tax_2017, tax_2016, tax_2015, tax_2014, " +
                        "debt_ratio_2018, debt_ratio_2017, debt_ratio_2016, debt_ratio_2015, debt_ratio_2014, " +
                        "profit_2018, profit_2017, profit_2016, profit_2015, profit_2014, " +
                        "to_ratio_2018, to_ratio_2017, to_ratio_2016, to_ratio_2015, to_ratio_2014, " +
                        "total_value, remission_2018, remission_2017, remission_2016, remission_2015, remission_2014, " +
                        "board_type, bachelor_num, master_num, doctor_num, lon, lat) values('" +
                        row.getCell(0).toString() + "', '" + row.getCell(1).toString() + "', '" + row.getCell(2).toString() + "', '" +
                        row.getCell(3).toString() + "', '" + row.getCell(4).toString() + "', '" + row.getCell(5).toString() + "', " +
                        row.getCell(6).toString() + ", '" + row.getCell(7).toString() + "', " + row.getCell(9).toString() + ", " +
                        row.getCell(10).toString() + ", " + row.getCell(11).toString() + ", " + row.getCell(12).toString() + ", " +
                        row.getCell(13).toString() + ", " + row.getCell(14).toString() + ", " + row.getCell(15).toString() + ", " +
                        row.getCell(16).toString() + ", " + row.getCell(17).toString() + ", " + row.getCell(18).toString() + ", " +
                        row.getCell(19).toString() + ", " + row.getCell(20).toString() + ", " + row.getCell(21).toString() + ", " +
                        row.getCell(22).toString() + ", " + row.getCell(23).toString() + ", " + row.getCell(24).toString() + ", " +
                        row.getCell(25).toString() + ", " + row.getCell(26).toString() + ", " + row.getCell(27).toString() + ", " +
                        row.getCell(28).toString() + ", " + row.getCell(29).toString() + ", " + row.getCell(30).toString() + ", " +
                        row.getCell(31).toString() + ", " + row.getCell(32).toString() + ", " + row.getCell(33).toString() + ", " +
                        row.getCell(34).toString() + ", " + row.getCell(35).toString() + ", " + row.getCell(36).toString() + ", " +
                        row.getCell(37).toString() + ", " + row.getCell(38).toString() + ", " + row.getCell(39).toString() + ", " +
                        row.getCell(40).toString() + ", " + row.getCell(41).toString() + ", " + row.getCell(42).toString() + ", " +
                        row.getCell(43).toString() + ", " + row.getCell(44).toString() + ", " + row.getCell(45).toString() + ", " +
                        row.getCell(46).toString() + ", " + row.getCell(47).toString() + ", " + row.getCell(48).toString() + ", " +
                        row.getCell(49).toString() + ", " + row.getCell(50).toString() + ", " + row.getCell(51).toString() + ", " +
                        row.getCell(52).toString() + ", " + row.getCell(53).toString() + ", " + row.getCell(54).toString() + ", " +
                        row.getCell(55).toString() + ", " + row.getCell(56).toString() + ", " + row.getCell(57).toString() + ", " +
                        row.getCell(58).toString() + ", " + row.getCell(59).toString() + ", '" + row.getCell(60).toString() + "', " +
                        row.getCell(61).toString() + ", " + row.getCell(62).toString() + ", " + row.getCell(63).toString() + ", " +
                        location.split(",")[0] + ", " + location.split(",")[1] + ")";
                sqlSentence = sqlSentence.replace(", ,", ", null,").replace(", ,", ", null,")
                        .replace(", '',", ", null,").replace(", '',", ", null,");
                ExecutorService executorService = Executors.newSingleThreadExecutor();
                executorService.submit(new DatabaseExecutor(sqlSentence, "listed-" + i + "-" + j));
                executorService.shutdown();
            }
        }
        workbook.close();
    }

    private static void addListedCompanyColumn() throws IOException, ClassNotFoundException, SQLException {
        String sqlSentence;
        sqlSentence = "alter table listed_company drop column if exists industrial_type";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
        sqlSentence = "alter table listed_company add industrial_type varchar(16)";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
        Workbook workbook = WorkbookFactory.create(new FileInputStream("misc/产业类型.xlsx"));
        ExecutorService executorService = Executors.newFixedThreadPool(maxThreads);
        for (int i = 0; i < workbook.getNumberOfSheets(); i += 1) {
            Sheet sheet = workbook.getSheetAt(i);
            for (int j = 1; j <= sheet.getLastRowNum(); j += 1) {
                Row row = sheet.getRow(j);
                if (row.getCell(1) != null) {
                    sqlSentence = "update listed_company set industrial_type = '" + row.getCell(0) +
                            "' where sec_code = '" + row.getCell(1) + "' and industrial_type is null";
                } else if (row.getCell(2) != null) {
                    sqlSentence = "update listed_company set industrial_type = '" + row.getCell(0) +
                            "' where sec_abbr_name = '" + row.getCell(2) + "' and industrial_type is null";
                } else if (row.getCell(3) != null) {
                    sqlSentence = "update listed_company set industrial_type = '" + row.getCell(0) +
                            "' where website = '" + row.getCell(3) + "' and industrial_type is null";
                }
                executorService.submit(new DatabaseExecutor(sqlSentence, "listed_add-" + i + "-" + j));
            }
        }
        executorService.shutdown();
        workbook.close();
    }

    private static void updateListedCompanyProvinceCity() throws ClassNotFoundException, SQLException {
        String sqlSentence;
        sqlSentence = "update listed_company set province = concat(province, '市') where province in ('上海', '北京', '重庆', '天津')";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
        sqlSentence = "update listed_company set city = concat(left(province, 2), '城区') where province in ('上海市', '北京市', '重庆市', '天津市') and right(city, 2) != '城区'";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
        sqlSentence = "update listed_company set city = '莱芜区' where city = '莱芜市'";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
    }

    private static void createShandongCompanyTable() throws ClassNotFoundException, SQLException {
        String sqlSentence;
        sqlSentence = "drop table if exists shandong_company";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
        /* WARNING: Don't import production description 'cause string format is awful and too long. */
        sqlSentence = "create table shandong_company(id serial primary key, name varchar(32), lg_psn_name varchar(32), " +
                "reg_cap varchar(32), est_date date, status varchar(8), province varchar(8), city varchar(8), county varchar(16), " +
                "company_type varchar(32), uscc varchar(32), tel varchar(32), tel_more varchar(128), address varchar(128), " +
                "website varchar(512), email varchar(128), production varchar(1024), lon float4, lat float4)";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
    }

    private static void importShandongCompanyData() throws IOException {
        String[] strings = {"misc/新一代信息技术合并去掉注资10万以下企业.xlsx", "misc/智能制造合并去掉注资10万以下企业.xlsx", "misc/海洋产业合并去掉注资10万以下企业.xlsx"};
        for (int fileIndex = 0; fileIndex < strings.length; fileIndex += 1) {
            Workbook workbook = WorkbookFactory.create(new FileInputStream(strings[fileIndex]));
            for (int i = 0; i < workbook.getNumberOfSheets(); i += 1) {
                Sheet sheet = workbook.getSheetAt(i);
                for (int j = 1; j <= sheet.getLastRowNum(); j += 1) {
                    Row row = sheet.getRow(j);
                    String location;
                    try {
                        URL url = new URL(address + "?address=" + row.getCell(12).toString().replace("#", "号").replace(" ", "") + "&key=" + key + "&batch=" + batch);
                        location = new ObjectMapper().readValue(url, ObjectNode.class).get("geocodes").get(0).get("location").asText();
                    } catch (NullPointerException e) {
                        System.out.println("#shandong-" + fileIndex + "-" + i + "-" + j + ": get location failed.");
                        continue;
                    } catch (IOException e) {
                        System.out.println("#shandong-" + fileIndex + "-" + i + "-" + j + ": unknown error.");
                        continue;
                    }
                    String sqlSentence = "insert into shandong_company(name, lg_psn_name, reg_cap, est_date, status, province, " +
                            "city, county, company_type, uscc, tel, tel_more, address, website, email, production, lon, lat) values('" +
                            row.getCell(0).toString() + "', '" + row.getCell(1).toString() + "', '" + row.getCell(2).toString() + "', '" +
                            row.getCell(3).toString() + "', '" + row.getCell(4).toString() + "', '" + row.getCell(5).toString() + "', '" +
                            row.getCell(6).toString() + "', '" + row.getCell(7).toString() + "', '" + row.getCell(8).toString() + "', '" +
                            row.getCell(9).toString() + "', '" + row.getCell(10).toString() + "', '" + row.getCell(11).toString() + "', '" +
                            row.getCell(12).toString() + "', '" + row.getCell(13).toString() + "', '" + row.getCell(14).toString() + "', '" +
                            row.getCell(15).toString() + "', " + location.split(",")[0] + ", " + location.split(",")[1] + ")";
                    sqlSentence = sqlSentence.replace(", ,", ", null,").replace(", ,", ", null,")
                            .replace(", '',", ", null,").replace(", '',", ", null,");
                    ExecutorService executorService = Executors.newSingleThreadExecutor();
                    executorService.submit(new DatabaseExecutor(sqlSentence, "shandong-" + fileIndex + "-" + i + "-" + j));
                    executorService.shutdown();
                }
            }
            workbook.close();
        }
    }

    private static void updateShandongCompanyProvince() throws ClassNotFoundException, SQLException {
        String sqlSentence;
        sqlSentence = "update shandong_company set province = concat(province, '省') where province = '山东'";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
    }

    private static void createShandongDigitalParkTable() throws ClassNotFoundException, SQLException {
        String sqlSentence;
        sqlSentence = "drop table if exists shandong_digital_park";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
        sqlSentence = "create table shandong_digital_park(id serial primary key, name varchar(16), province varchar(16), city varchar(16), lon float4, lat float4)";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
    }

    private static void importShandongDigitalParkData() throws IOException {
        Workbook workbook = WorkbookFactory.create(new FileInputStream("misc/山东省数字经济产业园区和数字经济企业.xlsx"));
        Sheet sheet = workbook.getSheetAt(0);
        for (int i = 1; i <= sheet.getLastRowNum(); i += 1) {
            Row row = sheet.getRow(i);
            String location;
            try {
                URL url = new URL(address + "?address=山东省" + row.getCell(0) + row.getCell(1).toString().replace("#", "号").replace(" ", "") + "&key=" + key + "&batch=" + batch);
                location = new ObjectMapper().readValue(url, ObjectNode.class).get("geocodes").get(0).get("location").asText();
            } catch (NullPointerException e) {
                System.out.println("#shandong-digital-0-" + i + ": get location failed.");
                continue;
            } catch (IOException e) {
                System.out.println("#shandong-digital-0-" + i + ": unknown error.");
                continue;
            }
            String sqlSentence = "insert into shandong_digital_park(name, province, city, lon, lat) values('" +
                    row.getCell(1).toString() + "', '山东省', '" + row.getCell(0) + "', " +
                    location.split(",")[0] + ", " + location.split(",")[1] + ")";
            sqlSentence = sqlSentence.replace(", ,", ", null,").replace(", ,", ", null,")
                    .replace(", '',", ", null,").replace(", '',", ", null,");
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            executorService.submit(new DatabaseExecutor(sqlSentence, "shandong-digital-0-" + i));
            executorService.shutdown();
        }
    }

    private static void createShandongDigitalCompanyTable() throws ClassNotFoundException, SQLException {
        String sqlSentence;
        sqlSentence = "drop table if exists shandong_digital_company";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
        /* WARNING: Don't import production description 'cause string format is awful and too long. */
        sqlSentence = "create table shandong_digital_company(id serial primary key, name varchar(32), lg_psn_name varchar(32), " +
                "reg_cap varchar(32), est_date date, status varchar(8), province varchar(8), city varchar(8), county varchar(16), " +
                "company_type varchar(32), uscc varchar(32), tel varchar(32), tel_more varchar(128), address varchar(128), " +
                "website varchar(512), email varchar(128), production varchar(1024), lon float4, lat float4)";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
    }

    private static void importShandongDigitalCompanyData() throws IOException {
        Workbook workbook = WorkbookFactory.create(new FileInputStream("misc/山东省数字经济产业园区和数字经济企业.xlsx"));
        Sheet sheet = workbook.getSheetAt(2);
        for (int i = 1; i <= sheet.getLastRowNum(); i += 1) {
            Row row = sheet.getRow(i);
            String location;
            try {
                URL url = new URL(address + "?address=" + row.getCell(12).toString().replace("#", "号").replace(" ", "") + "&key=" + key + "&batch=" + batch);
                location = new ObjectMapper().readValue(url, ObjectNode.class).get("geocodes").get(0).get("location").asText();
            } catch (NullPointerException e) {
                System.out.println("#shandong-digital-2-" + i + ": get location failed.");
                continue;
            } catch (IOException e) {
                System.out.println("#shandong-digital-2-" + i + ": unknown error.");
                continue;
            }
            String sqlSentence = "insert into shandong_digital_company(name, lg_psn_name, reg_cap, est_date, status, province, " +
                    "city, county, company_type, uscc, tel, tel_more, address, website, email, production, lon, lat) values('" +
                    row.getCell(0).toString() + "', '" + row.getCell(1).toString() + "', '" + row.getCell(2).toString() + "', '" +
                    row.getCell(3).toString() + "', '" + row.getCell(4).toString() + "', '" + row.getCell(5).toString() + "', '" +
                    row.getCell(6).toString() + "', '" + row.getCell(7).toString() + "', '" + row.getCell(8).toString() + "', '" +
                    row.getCell(9).toString() + "', '" + row.getCell(10).toString() + "', '" + row.getCell(11).toString() + "', '" +
                    row.getCell(12).toString() + "', '" + row.getCell(13).toString() + "', '" + row.getCell(14).toString() + "', '" +
                    row.getCell(15).toString() + "', " + location.split(",")[0] + ", " + location.split(",")[1] + ")";
            sqlSentence = sqlSentence.replace(", ,", ", null,").replace(", ,", ", null,")
                    .replace(", '',", ", null,").replace(", '',", ", null,")
                    .replace(", '-',", ", null,").replace(", '-',", ", null,");
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            executorService.submit(new DatabaseExecutor(sqlSentence, "shandong-digital-2-" + i));
            executorService.shutdown();
        }
        workbook.close();
    }

    private static void createDistrictBoundaryTable() throws ClassNotFoundException, SQLException{
        String sqlSentence;
        sqlSentence = "drop table if exists district_boundary";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
        sqlSentence = "create table district_boundary(province varchar(32), city varchar(32), district varchar(32))";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
        sqlSentence = "alter table district_boundary add constraint pk primary key(province, city, district)";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
        sqlSentence = "select addgeometrycolumn('district_boundary', 'geom', 4326, 'multipolygon', 2)";
        ResultSet resultSet = DatabaseExecutor.executeQuery(sqlSentence);
        resultSet.next();
        System.out.println(resultSet.getString("addgeometrycolumn") + " on {" + sqlSentence + "}");
    }

    private static void importDistrictBoundaryData() throws NullPointerException {
        ObjectNode countryObjectNode = AMapGetDistrict.getDistrict("中华人民共和国", 1, "all");
        if (countryObjectNode == null) {
            throw new NullPointerException();
        }
        String countryPolyline = countryObjectNode.get("districts").get(0).get("polyline").asText();
        String countrySqlSentence = "insert into district_boundary(province, city, district, geom) values('" +
                "全部', '全部', '全部', st_geomfromtext('multipolygon(((" + countryPolyline.replace(",", " ").replace(";", ", ").replace("|", ")), ((") + ")))', 4326))";
        ExecutorService countryExecutorService = Executors.newSingleThreadExecutor();
        countryExecutorService.submit(new DatabaseExecutor(countrySqlSentence, "全部-全部-全部"));
        countryExecutorService.shutdown();
        ArrayNode provinceArrayNode = (ArrayNode)countryObjectNode.get("districts").get(0).get("districts");
        for (int i = 0; i < provinceArrayNode.size(); i += 1) {
            String provinceName = provinceArrayNode.get(i).get("name").asText();
            String provinceCode = provinceArrayNode.get(i).get("adcode").asText();
            ObjectNode provinceObjectNode = AMapGetDistrict.getDistrict(provinceCode, 1, "all");
            if (provinceObjectNode == null) {
                throw new NullPointerException();
            }
            String provincePolyline = provinceObjectNode.get("districts").get(0).get("polyline").asText();
            String provinceSqlSentence = "insert into district_boundary(province, city, district, geom) values('" +
                    provinceName + "', '全部', '全部', st_geomfromtext('multipolygon(((" + provincePolyline.replace(",", " ").replace(";", ", ").replace("|", ")), ((") + ")))', 4326))";
            ExecutorService provinceExecutorService = Executors.newSingleThreadExecutor();
            provinceExecutorService.submit(new DatabaseExecutor(provinceSqlSentence, provinceName + "-全部-全部"));
            provinceExecutorService.shutdown();
            ArrayNode cityArrayNode = (ArrayNode)provinceObjectNode.get("districts").get(0).get("districts");
            for (int j = 0; j < cityArrayNode.size(); j += 1) {
                String cityName = cityArrayNode.get(j).get("name").asText();
                String cityCode = cityArrayNode.get(j).get("adcode").asText();
                ObjectNode cityObjectNode = AMapGetDistrict.getDistrict(cityCode, 1, "all");
                if (cityObjectNode == null) {
                    throw new NullPointerException();
                }
                String cityPolyline = cityObjectNode.get("districts").get(0).get("polyline").asText();
                String citySqlSentence = "insert into district_boundary(province, city, district, geom) values('" +
                        provinceName + "', '" + cityName + "', '全部', st_geomfromtext('multipolygon(((" + cityPolyline.replace(",", " ").replace(";", ", ").replace("|", ")), ((") + ")))', 4326))";
                ExecutorService cityExecutorService = Executors.newSingleThreadExecutor();
                cityExecutorService.submit(new DatabaseExecutor(citySqlSentence, provinceName + "-" + cityName + "-全部"));
                cityExecutorService.shutdown();
                ArrayNode districtArrayNode = (ArrayNode)cityObjectNode.get("districts").get(0).get("districts");
                for (int k = 0; k < districtArrayNode.size(); k += 1) {
                    String districtName = districtArrayNode.get(k).get("name").asText();
                    String districtCode = districtArrayNode.get(k).get("adcode").asText();
                    ObjectNode districtObjectNode = AMapGetDistrict.getDistrict(districtCode, 0, "all");
                    if (districtObjectNode == null) {
                        throw new NullPointerException();
                    }
                    String districtPolyline = districtObjectNode.get("districts").get(0).get("polyline").asText();
                    String districtSqlSentence = "insert into district_boundary(province, city, district, geom) values('" +
                            provinceName + "', '" + cityName + "', '" + districtName + "', st_geomfromtext('multipolygon(((" + districtPolyline.replace(",", " ").replace(";", ", ").replace("|", ")), ((") + ")))', 4326))";
                    ExecutorService districtExecutorService = Executors.newSingleThreadExecutor();
                    districtExecutorService.submit(new DatabaseExecutor(districtSqlSentence, provinceName + "-" + cityName + "-" + districtName));
                    districtExecutorService.shutdown();
                }
            }
        }
    }

    private static void createHebeiClusterTable() throws ClassNotFoundException, SQLException {
        String sqlSentence;
        sqlSentence = "drop table if exists hebei_cluster";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
        sqlSentence = "create table hebei_cluster(id serial primary key, city varchar(16), cluster_name varchar(64), district varchar(16), " +
                "industrial_type varchar(32), production varchar(64), produce_company_num int, matched_company_num int, related_company_num int, income int)";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
    }

    private static void importHebeiClusterData() throws IOException {
        Workbook workbook = WorkbookFactory.create(new FileInputStream("misc/河北省产业集群.xlsx"));
        Sheet sheet = workbook.getSheetAt(0);
        ExecutorService executorService = Executors.newFixedThreadPool(maxThreads);
        for (int i = 1; i <= sheet.getLastRowNum(); i += 1) {
            Row row = sheet.getRow(i);
            String sqlSentence = "insert into hebei_cluster(city, cluster_name, district, industrial_type, production, produce_company_num, matched_company_num, related_company_num, income) values('" +
                    row.getCell(1).toString().replace(" ", "") + "市', '" + row.getCell(2).toString().replace(" ", "") + "', '" +
                    row.getCell(3).toString().replace(" ", "") + "', '" + row.getCell(4).toString().replace(" ", "") + "', '" +
                    row.getCell(5).toString().replace(" ", "") + "', " + row.getCell(6).toString() + ", " +
                    row.getCell(7).toString() + ", " + row.getCell(8).toString() + ", " + row.getCell(9).toString() + ")";
            executorService.submit(new DatabaseExecutor(sqlSentence, "hebei-cluster-0-" + i));
        }
        executorService.shutdown();
        workbook.close();
    }

    private static void createFiveHundredTable() throws ClassNotFoundException, SQLException {
        String sqlSentence;
        sqlSentence = "drop table if exists five_hundred";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
        sqlSentence = "create table five_hundred(id serial primary key, rank_2018 int, rank_2017 int, name varchar(64), " +
                "income float4, profit float4, address varchar(128), chairman varchar(8), sec_code varchar(16), employee_num int, " +
                "code varchar(32), website varchar(128), pure_assets float, assets float, value float, pure_profit_rate float, " +
                "pure_assets_rate float, income_rate float, profit_rate float, lon float4, lat float4)";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
    }

    private static void importFiveHundredTable() throws IOException {
        Workbook workbook = WorkbookFactory.create(new FileInputStream("misc/2018年中国500强排行榜.xlsx"));
        Sheet sheet = workbook.getSheetAt(0);
        ExecutorService executorService = Executors.newFixedThreadPool(maxThreads);
        for (int i = 1; i <= sheet.getLastRowNum(); i += 1) {
            Row row = sheet.getRow(i);
            String location;
            try {
                URL url = new URL(address + "?address=" + row.getCell(5).toString().replace("#", "号").replace(" ", "") + "&key=" + key + "&batch=" + batch);
                location = new ObjectMapper().readValue(url, ObjectNode.class).get("geocodes").get(0).get("location").asText();
            } catch (NullPointerException e) {
                System.out.println("#five-hundred-0-" + i + ": get location failed.");
                continue;
            } catch (IOException e) {
                System.out.println("#five-hundred-0-" + i + ": unknown error.");
                continue;
            }
            String sqlSentence = "insert into five_hundred(rank_2018, rank_2017, name, income, profit, address, chairman, " +
                    "sec_code, employee_num, code, website, pure_assets, assets, value, pure_profit_rate, pure_assets_rate, " +
                    "income_rate, profit_rate, lon, lat) values(" + row.getCell(0).toString() + ", " +
                    row.getCell(1).toString() + ", '" + row.getCell(2).toString() + "', " +
                    row.getCell(3).toString() + ", " + row.getCell(4).toString() + ", '" + row.getCell(5).toString() + "', '" +
                    row.getCell(6).toString() + "', '" + row.getCell(7).toString() + "', " + row.getCell(8).toString().replace(",", "") + ", '" +
                    row.getCell(9).toString() + "', '" + row.getCell(10).toString() + "', " +
                    row.getCell(11).toString().replace(",", "") + ", " +
                    row.getCell(12).toString().replace(",", "") + ", " +
                    row.getCell(13).toString().replace(",", "") + ", " +
                    row.getCell(14).toString().replace(",", "") + ", " +
                    row.getCell(15).toString().replace(",", "") + ", " +
                    row.getCell(16).toString().replace(",", "") + ", " +
                    row.getCell(17).toString().replace(",", "") + ", " +
                    location.split(",")[0] + ", " + location.split(",")[1] + ")";
            sqlSentence = sqlSentence.replace("'--'", "null").replace("'空'", "null")
                    .replace("'-'", "null").replace("--,", "null,")
                    .replace("-,", "null,").replace("空,", "null,");
            executorService.submit(new DatabaseExecutor(sqlSentence, "five-hundred-0-" + i));
        }
        executorService.shutdown();
        workbook.close();
    }

    private static void createCustomRegionTable() throws ClassNotFoundException, SQLException {
        String sqlSentence;
        sqlSentence = "drop table if exists custom_region";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
        sqlSentence = "create table custom_region(id serial primary key, name varchar(32))";
        System.out.println(DatabaseExecutor.executeUpdate(sqlSentence) + " on {" + sqlSentence + "}");
        sqlSentence = "select addgeometrycolumn('custom_region', 'geom', 4326, 'multipolygon', 2)";
        ResultSet resultSet = DatabaseExecutor.executeQuery(sqlSentence);
        resultSet.next();
        System.out.println(resultSet.getString("addgeometrycolumn") + " on {" + sqlSentence + "}");
    }
}

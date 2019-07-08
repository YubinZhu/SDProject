package backendScripts;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by yubzhu on 2019/7/7
 */

public class ImportData {

    public static void main(String[] args) {
        try {
            createListedCompanyTable();
            importListedCompanyData();
        } catch (ClassNotFoundException | SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    private static void createListedCompanyTable() throws ClassNotFoundException, SQLException {
        Tools.execute("drop table if exists listed_company");
        Tools.execute("create table listed_company (id serial primary key, sec_code char(16), sec_abbr_name char(16), com_chn_name char(32), " +
                "province char(16), website char(64), address char(128), employee_num int, city char(16), " +
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
                "board_type char(8), bachelor_num int, master_num int, doctor_num int, lon float4, lat float4)");
    }

    private static void importListedCompanyData() throws IOException, ClassNotFoundException, SQLException {
        Workbook workbook = WorkbookFactory.create(new FileInputStream("misc/全部A股+科创板+新三板.xlsx"));
        for (int i = 0; i < workbook.getNumberOfSheets(); i += 1) {
            Sheet sheet = workbook.getSheetAt(i);
            for (int j = 1; j <= sheet.getLastRowNum(); j += 1) {
                Row row = sheet.getRow(j);
                String location = Tools.getLocation(row.getCell(5).toString().replace("#", "号"));
                if (location == null) {
                    continue;
                }
                /* WARNING: Don't import production description 'cause string format is awful and too long. */
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
                        location.split(",")[0].split("\"")[1] + ", " + location.split(",")[1].split("\"")[0] + ")";
                sqlSentence = sqlSentence.replace(", ,", ", null,").replace(", ,", ", null,");
                System.out.println("#" + i + "-" + j + ": " + sqlSentence);
                Tools.execute(sqlSentence);
            }
        }
    }
}

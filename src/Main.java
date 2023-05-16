
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.commons.lang3.ObjectUtils;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseBool;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.constraint.UniqueHashCode;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        System.out.println("Start time = "+ LocalDateTime.now());
        String fileName = "C:\\Users\\cnason\\OneDrive - US-Analytics\\work documents\\datamodel\\Anderson Holdings\\configuration\\Source-Target_Mapping_dv.csv";
        ICsvMapReader mapReader = null;
        Map<String, Object> customerMap = null;
        try {
            mapReader = new CsvMapReader(new FileReader(fileName), CsvPreference.STANDARD_PREFERENCE);

            // the header columns are used as the keys to the Map
            final String[] header = mapReader.getHeader(true);
            final CellProcessor[] processors = getProcessors();

            while ((customerMap = mapReader.read(header, processors)) != null) {
                String stg_st = "'TABLE', '" + customerMap.get("Stg Physical Table") + "', '" + customerMap.get("Stg Physical Field") + "', '" + customerMap.get("Stg Datatype")
                        + "', '" + customerMap.get("Stg Field Nullable") + "', '" + customerMap.get("Stg Precision") + "', '" + customerMap.get("Stg Scale") + "', '" + customerMap.get("Stg Definition")
                        + "', '" + customerMap.get("Stg Transformation")+ "', '" + customerMap.get("Stg Comments")+ "', " + "'OA_PLAN_STG', '" + customerMap.get("Mapper")+ "'";
                //System.out.println(stg_st);
                String dv_st = "'TABLE', '" + customerMap.get("DV Physical Table") + "', '" + customerMap.get("DV Physical Field") + "', '" + customerMap.get("DV Datatype")
                        + "', '" + customerMap.get("DV Field Nullable") + "', '" + customerMap.get("DV Precision") + "', '" + customerMap.get("DV Scale") + "', '" + customerMap.get("DV Definition")
                        + "', '" + customerMap.get("DV Transformation")+ "', '" + customerMap.get("DV Comments")+ "', " + "'OA_PLAN_ODS', '" + customerMap.get("Mapper")+"'";
                //System.out.println(customerMap.keySet());
                //System.out.println(stg_st);
                //System.out.println(dv_st);
                if(customerMap.get("Stg Physical Table")!= null) {
                    writeToCfgTable(stg_st);
                }
                if(customerMap.get("DV Physical Table") != null) {
                    writeToCfgTable(dv_st);
                }
            }
            mapReader.close();

        } catch (IOException e) {
            System.out.println("error: " + e);
            System.exit(-1);

        }
        System.out.println("End time = "+ LocalDateTime.now());
    }
    public static void writeToCfgTable(String st){
        String dbUrl= "jdbc:oracle:thin:@oas.usa-env.com:1521/orclpdb";
        String user = "OA_PLAN_STG";
        String pwd = "STG_Pl#n4oa5";

        try{
            Connection con1= DriverManager.getConnection(dbUrl,user,pwd);
            String sql = "insert into OA_PLAN_STG.INFINITY_CFG_DV (OBJECT_TYPE,OBJECT_NAME,COLUMN_NAME,DATA_TYPE, FIELD_NULLABLE,PRECISION,SCALE,DEFN,TRANS,CMNTS,OBJECT_SCHEMA,MAPPER,UPDATE_FIELD,LOAD_DTS) VALUES (" + st + ",'I',sysdate)";
            //System.out.println(sql);
            Statement s1 = con1.createStatement();
            s1.executeUpdate(sql);
            con1.close();
        } catch(SQLException e){
            System.out.println("error: "+e);
            System.exit(-1);
        }
    }



    private static CellProcessor[] getProcessors() {
        final CellProcessor[] processors = new CellProcessor[] {
                new UniqueHashCode(), // id (must be unique)
                new Optional(), // Stg Physical Table
                new Optional(), // Stg Physical Field
                new Optional((new ParseBool())), // Stg Field Nullable
                new Optional(), // Stg Datatype
                new Optional(), // Stg Precision
                new Optional(), // Stg Scale
                new Optional(), // Stg Definition
                new Optional(), // Stg Transform
                new Optional(), // Stg Comment
                new Optional(), // DV Physical Table
                new Optional(), // DV Physical Field
                new Optional((new ParseBool())), // DV Field Nullable
                new Optional(), // DV Datatype
                new Optional(), // DV Precision
                new Optional(), // DV Scale
                new Optional(), // DV Definition
                new Optional(), // DV Comment
                new Optional(), // DV Transform
                new Optional(), // Mapper
                new Optional(new ParseDate("dd/MM/yyyy")), // Last Change Date
                new Optional() // Last Change Description
        };

        return processors;
    }
}
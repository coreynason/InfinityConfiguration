import java.sql.*;
import java.time.LocalDateTime;


public class CreateTables {
    public static void main() {
        String dbUrl = "jdbc:oracle:thin:@oas.usa-env.com:1521/orclpdb";
        String user = "OA_PLAN_STG";
        String pwd = "STG_Pl#n4oa5";

        try {
            Connection con = DriverManager.getConnection(dbUrl, user, pwd);
            Connection con2 = DriverManager.getConnection(dbUrl, user, pwd);
            String aksql = "select DISTINCT OBJECT_NAME, OBJECT_SCHEMA from OA_PLAN_STG.INFINITY_CFG_DV Where OBJECT_NAME in (select TABLE_NAME from all_tables)";
            //System.out.println(aksql);
            Statement cst = con.createStatement();
            ResultSet rc = cst.executeQuery(aksql);
            while (rc.next()) {
                String tname2 = rc.getString("OBJECT_NAME");
                //System.out.println("Altering table " + tname2);
                if (tname2 == null) {
                    System.out.println("done");
                } else {
                    alterTable(dbUrl, user, pwd, tname2);
                }
            }
            String csql = "select DISTINCT OBJECT_NAME, OBJECT_SCHEMA from OA_PLAN_STG.INFINITY_CFG_DV Where OBJECT_NAME not in (select TABLE_NAME from all_tables)";
            //System.out.println(csql);
            Statement s = con2.createStatement();
            ResultSet r = s.executeQuery(csql);
                 while (r.next()) {
                    String tname1 = r.getString("OBJECT_NAME");
                    String os = r.getString("OBJECT_SCHEMA");
                    //System.out.println("Creating table " + tname1);
                    //System.out.println("Table " + tname1);
                    if (tname1 == null) {
                        System.out.println("done");
                    } else {
                        createTable(dbUrl, user, pwd, os, tname1);
                    }
                }

            con.close();
            con2.close();
            updateLoadDate(dbUrl,user,pwd);
        } catch (SQLException e) {
            if(e.getMessage().contains("No data read")){
                System.out.println("'No New columns");
                updateLoadDate(dbUrl,user,pwd);

            }else {
                System.out.println("error: " + e);
                System.exit(-1);
            }
        }
    }
    public static void createTable(String dbUrl,String user,String pwd,String schema ,String tname) {

        try {
            Connection con3 = DriverManager.getConnection(dbUrl, user, pwd);
            Statement s1 = con3.createStatement();
            String csql = "create table "+ schema + "." + tname + " (dummy varchar2(1))";
            s1.executeQuery(csql);
            //System.out.println("Created: "+tname);
            alterTable(dbUrl,user,pwd,tname);
            System.out.println("Added all Colmuns");
            String dummy = "Alter Table " + schema + "." + tname + " DROP COLUMN dummy";
            //System.out.println(dummy);
            Statement s4 = con3.createStatement();
            ResultSet r4 = s4.executeQuery(dummy);
            con3.close();
        } catch (SQLException e) {
            System.out.println("error: " + e);
            System.exit(-1);
        }
    }
    public  static void alterTable (String dbUrl,String user,String pwd,String tableName){

        try {
            Connection con4 = DriverManager.getConnection(dbUrl, user, pwd);
            Connection con5 = DriverManager.getConnection(dbUrl, user, pwd);
            String sql2 = "select OBJECT_NAME,COLUMN_NAME, DATA_TYPE,PRECISION,SCALE,OBJECT_SCHEMA from OA_PLAN_STG.INFINITY_CFG_DV Where OBJECT_TYPE = 'TABLE' and OBJECT_NAME = '" + tableName+"' and column_name not in (SELECT COLUMN_NAME FROM all_tab_columns WHERE  table_name = '"+ tableName + "')";
            //System.out.println(sql2);
            Statement s2 = con4.createStatement();
            ResultSet r2 = s2.executeQuery(sql2);
            while (r2.next()) {
                String tName = r2.getString("OBJECT_NAME");
                String cName = r2.getString("COLUMN_NAME");
                String dtype = "";
                //System.out.println(prec);
                //System.out.println(sc);
                switch (r2.getString("DATA_TYPE")) {
                    case "TIMESTAMP":
                        dtype = r2.getString("DATA_TYPE");
                        break;
                    case "VARCHAR2":
                        dtype = r2.getString("DATA_TYPE") + "(" + r2.getString("PRECISION") + ")";
                        break;
                    default:
                        dtype = r2.getString("DATA_TYPE") + "(" + r2.getString("PRECISION") + "," + r2.getString("SCALE") + ")";
                        break;
                }
                String schema = r2.getString("OBJECT_SCHEMA");
                String asql = "Alter Table " + schema + "." + tName + " ADD " + '"'+cName + '"'+ " "  + dtype;
                //System.out.println(asql);
                Statement s3 = con5.createStatement();
                ResultSet r3 = s3.executeQuery(asql);
            }
            con4.close();
            con5.close();
        } catch (SQLException e) {
            System.out.println("error: " + e);
            System.exit(-1);
        }
    }
    public static void updateLoadDate(String dbUrl,String user,String pwd){
        try {
            Connection con6 = DriverManager.getConnection(dbUrl, user, pwd);
            Connection con7 = DriverManager.getConnection(dbUrl, user, pwd);
            String csql = "select DISTINCT OBJECT_NAME, OBJECT_SCHEMA from OA_PLAN_STG.INFINITY_CFG_DV";
            Statement s5 = con6.createStatement();
            Statement s6 = con7.createStatement();
            ResultSet r5 = s5.executeQuery(csql);
            while (r5.next()) {
                String tname = r5.getString("OBJECT_NAME");
                String os = r5.getString("OBJECT_SCHEMA");
                String tssql = "Alter table " + os+"."+tname + " MODIFY LOAD_DTS Default sysdate";
                ResultSet r6 = s6.executeQuery(tssql);
                //System.out.println(tssql);
            }
        }catch (SQLException e) {
                throw new RuntimeException(e);
            }
    }
}

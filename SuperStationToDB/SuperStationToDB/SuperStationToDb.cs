using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data;
using System.Data.OleDb;
using Readearth.Data;
using ChinaAirUtility;

namespace Utility.SuperStation
{
    public class SuperStation
    {
        static string ConnectionStrings = "";
        static Database m_Database;
        public string Station { get; set; }
        public string City { get; set; }
        public string Type { get; set; }

        public SuperStation(string connectionstrings) {
            ConnectionStrings = connectionstrings;
            m_Database = new Database(ConnectionStrings);
        }

        public int InsertToDB( DateTime LST, string filepath)
        {
            try
            {
                if (Type == "Hmetal")
                {
                    string file = filepath + LST.ToString("yyyy") + "/" + LST.ToString("yyyyMMdd") + "/" + LST.ToString("yyyyMMddHHmm") + ".txt";
                    if (File.Exists(file))
                    {
                        if (LST.Minute > 30)
                            LST = Convert.ToDateTime(LST.ToString("yyyy-MM-dd HH:00:00")).AddHours(1);
                        else
                            LST = Convert.ToDateTime(LST.ToString("yyyy-MM-dd HH:00:00"));
                        Update.UpdateDb(ToCSV(file, ','), Type, LST, City, Station);
                    }
                }
                else if (Type == "颗粒")
                {
                    DataTable Table = ToCSV(filepath, ',');
                    foreach (DataRow row in Table.Rows)
                    {
                        Update.UpdateVarDb(row, Station, Type, City, "T_SuperStation");
                    }
                }
                else if (Type == "VOCs")
                {
                    DataTable Table = ToCSV(filepath, ',');
                    foreach (DataRow row in Table.Rows)
                    {
                        Update.UpdateVOCDb(row, Station, Type, City, "T_SuperStation");
                    }
                }
                else if (Type == "OMI")
                {
                    ExternalTable.O3Lidar O3Leida = new ExternalTable.O3Lidar(m_Database);
                    DataTable sources = O3Leida.MonthColumnO3toDb(filepath, Type, Station, City);
                    foreach (DataRow row in sources.Rows)
                    {
                        Update.UpdateVarDb(row, Station, Type, City, "T_SuperStation");
                    }
                }
                //新增*
                #region type：BC

                else if (Type == "BC")
                {
                    BC IR1 = new BC();
                    //string folder = @"D:\1上海地听\环科院\超级站黑炭\Data_BC";//数据源路径
                    DateTime maxtime;
                    try
                    {
                        maxtime =
                            Convert.ToDateTime(
                                m_Database.GetFirstValue("select max(StartTime) from  T_SuperStationmin where  Type='" +
                                                         Type + "'"));
                    }
                    catch
                    {
                        maxtime = LST.AddHours(-72);
                    } //自定义第一次入库时间
                    //分钟数据入库
                    DataTable IR1soueces = IR1.GetIR1source(filepath, maxtime);
                    foreach (DataRow row in IR1soueces.Rows)
                    {
                        DateTime start = Convert.ToDateTime(row[0].ToString());
                        Update.UpdateVarDb(row, Station, Type, City, "T_SuperStationmin");
                        string strlasttime =
                            m_Database.GetFirstValue("select max(StartTime) from  T_SuperStation where  Type='" + Type +
                                                     "'");
                        //小时数据入库
                        DateTime lasttime = Convert.ToDateTime(maxtime.AddHours(-72).ToString("yyyy-MM-dd HH:00:00"));
                        if (strlasttime != "")
                            lasttime = Convert.ToDateTime(strlasttime);
                        for (DateTime dtime = lasttime.AddHours(1);
                            dtime < Convert.ToDateTime(start.ToString("yyyy-MM-dd HH:00:00"));
                            dtime = dtime.AddHours(1))
                        {
                            string Str_SQL = "select '" + dtime + "' as [StartTime]  ,'" + dtime.AddHours(1) +
                                             "' as [EndTime]  ,[City]  ,[Station]  ,[Item] ,avg([Value]) as [Value]  ,'' as [Quality]  ,[Type]  ,avg([DValue]) as [DValue]  ,'' as [OPCode] ,'' as [LMark]  ,'' as [HMark] ,'' as [State] from T_SuperStationmin where convert(varchar(13),StartTime,120)+':00:00'='" +
                                             dtime.ToString("yyyy-MM-dd HH:00:00") + "'  and   Type='" + Type +
                                             "'  and  [DValue] !=-99 and  [Value]!=-99 group by  [City]  ,[Station], [Type],[Item] ";

                            DataTable Table_Data = m_Database.GetDataset(Str_SQL).Tables[0];
                            if (Table_Data.Rows.Count != 0)
                                Update.UpdateDb(Table_Data, Type, dtime, City, Station);
                            //lasttime = dtime;
                        }
                    }
                }
                #endregion

                else if (Type == "Sourcelist")
                {
                    try
                    {
                        string insertsql =
                            "insert into T_SourceList ([Year]  ,[Code]  ,[ParameterID]  ,[TypeCode]   ,[CLType]  ,[EcoCode]  ,[Value]) values ";
                        DataTable Table = ToCSV(filepath, ',');
                        int typecode = (int)Enum.Parse(typeof(TypeCode), Type);
                        int i = 0;
                        foreach (DataRow row in Table.Rows)
                        {
                            int year = Convert.ToInt32(row[0]);
                            City = row[1].ToString();
                            string province = row[2].ToString();
                            int code = GetCityCode(City, province);
                            int cltype = -1;
                            int ecocode = -1;
                            for (int j = 3; j < Table.Columns.Count; j++)
                            {
                                if (Table.Columns[j].ColumnName.Contains("Column"))
                                    continue;
                                double value = 0;
                                try
                                {
                                    value = Convert.ToDouble(row[j]);
                                }
                                catch
                                {
                                }
                                int parameterid = GetParameterID(Table.Columns[j].ColumnName);
                                insertsql += string.Format("({0},{1},{2},{3},{4},{5},{6}),", year, code, parameterid,
                                    typecode, cltype, ecocode, value);
                                i++;
                                if (i > 900)
                                {
                                    m_Database.Execute(insertsql.Remove(insertsql.Length - 1));
                                    insertsql =
                                        "insert into T_SourceList ([Year]  ,[Code]  ,[ParameterID]  ,[TypeCode]   ,[CLType]  ,[EcoCode]  ,[Value]) values ";
                                    i = 0;
                                }
                            }
                        }
                        m_Database.Execute(insertsql.Remove(insertsql.Length - 1));
                    }
                    catch
                    {
                    }
                }
                return 2;
            }
            catch { return -1; }
        }

        private class Update
        {
            /// <summary>
            /// 超级站同表入库
            /// </summary>
            /// <param name="result"></param>
            /// <param name="type"></param>
            /// <param name="lst"></param>
            /// <param name="city"></param>
            /// <param name="station"></param>
            public static void UpdateDb(DataTable result, string type, DateTime lst, string city, string station)
            {
                string deletesql = string.Format("delete  T_SuperStation where StartTime='{0}' and  City='{1}' and  Station='{2}' and  Type='{3}'", lst, city, station, type);
                m_Database.Execute(deletesql);
                string sqlinsert = "";
                foreach (DataRow dr in result.Rows)
                {
                    try
                    {
                        sqlinsert = sqlinsert + string.Format("insert into T_SuperStation (StartTime,EndTime,City,Station,Item,Value,Quality,Type,DValue, OPCode) values('{0}','{1}','{2}','{3}',{4},{5},'{6}','{7}',{8},'{9}');", lst, lst.AddHours(1), dr[2], dr[3], dr[4], dr[5], dr[6], dr[7], dr[8], dr[9]);
                    }
                    catch { }
                }
                m_Database.Execute(sqlinsert);
            }
            /// <summary>
            /// 雷达同表入库
            /// </summary>
            /// <param name="result"></param>
            /// <param name="type"></param>
            /// <param name="lst"></param>
            /// <param name="city"></param>
            /// <param name="station"></param>
            public static void InsertLidarToDB(DataTable result, string type, DateTime lst)
            {
                try
                {
                    string deletesql = string.Format("delete  T_Lidar where LST='{0}' and  type like '{1}%' ", lst, type);
                    m_Database.Execute(deletesql);
                    string sqlinsert = "";
                    foreach (DataRow dr in result.Rows)
                    {

                        sqlinsert = sqlinsert + string.Format("insert into T_Lidar  values({0},'{1}',{2},'{3}');", dr[0], lst, dr[1], type + dr[2]);

                    }
                    m_Database.Execute(sqlinsert);
                }
                catch { }
            }
            /// <summary>
            /// 更新颗粒数据
            /// </summary>
            /// <param name="row"></param>
            /// <param name="station"></param>
            /// <param name="type"></param>
            public static void UpdateVarDb(DataRow row, string station, string type, string city,string tablename)
            {
                DateTime start = Convert.ToDateTime(row[0].ToString());
                DateTime end = Convert.ToDateTime(row[0].ToString());
                Dictionary<string, SuperItem> Items = new Dictionary<string, SuperItem>();

                DataTable Table_Item = m_Database.GetDataset("select * from D_ItemCode").Tables[0];
                int maxdm = Convert.ToInt32(m_Database.GetFirstValue("  select MAX(DM) from  D_ItemCode"));

                int superstationCount = Convert.ToInt32(m_Database.GetFirstValue("if exists (SELECT TOP 1 1 FROM " + tablename + " where StartTime = '" + start + "' and  Type ='" + type + "' and Station='" + station + "' ) SELECT 1 ELSE SELECT 0"));
                if (superstationCount != 0)
                    m_Database.Execute("delete " + tablename + " where StartTime = '" + start + "' and  Type ='" + type + "' and Station='" + station + "'");

                for (int i = 1; i < row.Table.Columns.Count; i++)
                {
                    string colummname = row.Table.Columns[i].ColumnName;
                    if (colummname.Contains("Column"))
                        continue;
                    int itemcode = -1;
                    double value = Convert.ToDouble(row[i].ToString() != "" ? row[i].ToString() : "999");
                    if (value == 999)
                        value = -99;
                    try
                    {
                        itemcode = int.Parse(Table_Item.Select("MC = '" + colummname + "'")[0]["DM"].ToString());
                    }
                    catch
                    {
                        maxdm = maxdm + 1;
                        string insertsql = " insert into D_ItemCode ([DM],[MC]  ,[Type])  values (" + maxdm + ",'" + colummname + "','" + type + "') ; insert into D_ModelAnalysis_Item ([DM],[MC]  ,[DP])  values (" + maxdm + ",'" + colummname + "','" + type + "')";
                        m_Database.Execute(insertsql);
                        itemcode = maxdm;
                    }
                    SuperItem item = new SuperItem("", value, itemcode, "");
                    Items.Add(colummname, item);
                }
                string sqlinsert = "";
                foreach (SuperItem item in Items.Values)
                {
                    sqlinsert = sqlinsert + string.Format("insert into " + tablename + " (StartTime,EndTime,City,Station,Item,Value,Quality,Type,DValue) values('{0}','{1}','{2}','{3}',{4},{5},'{6}','{7}',{8});", start, end, city, station, item.Item, item.Value, item.Quality, type, item.Value);
                }
                m_Database.Execute(sqlinsert);
            }
            /// <summary>
            /// 更新颗粒数据
            /// </summary>
            /// <param name="row"></param>
            /// <param name="station"></param>
            /// <param name="type"></param>
            public static void UpdateVOCDb(DataRow row, string station, string type, string city, string tablename)
            {
                DateTime start = Convert.ToDateTime(row[0].ToString());
                DateTime end = Convert.ToDateTime(row[0].ToString());
                Dictionary<string, SuperItem> Items = new Dictionary<string, SuperItem>();

                DataTable Table_Item = m_Database.GetDataset("select * from D_VOCItem").Tables[0];
                int maxdm = Convert.ToInt32(m_Database.GetFirstValue("  select MAX(DM) from  D_ItemCode"));

                int superstationCount = Convert.ToInt32(m_Database.GetFirstValue("if exists (SELECT TOP 1 1 FROM " + tablename + " where StartTime = '" + start + "' and  Type ='" + type + "' and Station='" + station + "' ) SELECT 1 ELSE SELECT 0"));
                if (superstationCount != 0)
                    m_Database.Execute("delete " + tablename + " where StartTime = '" + start + "' and  Type ='" + type + "' and Station='" + station + "'");

                for (int i = 1; i < row.Table.Columns.Count; i++)
                {
                    string colummname = row.Table.Columns[i].ColumnName;
                    if (colummname.Contains("Column"))
                        continue;
                    int itemcode = -1;
                    double value = Convert.ToDouble(row[i].ToString() != "" ? row[i].ToString() : "999");
                    if (value == 999)
                        value = -99;
                    try
                    {
                        itemcode = int.Parse(Table_Item.Select("Name = '" + colummname + "'")[0]["Item"].ToString());
                    }
                    catch
                    {
                        maxdm = maxdm + 1;
                        string insertsql = " insert into D_ItemCode ([DM],[MC]  ,[Type])  values (" + maxdm + ",'" + colummname + "','" + type + "') ; insert into D_ModelAnalysis_Item ([DM],[MC]  ,[DP])  values (" + maxdm + ",'" + colummname + "','" + type + "')";
                        m_Database.Execute(insertsql);
                        itemcode = maxdm;
                    }
                    SuperItem item = new SuperItem("", value, itemcode, "");
                    Items.Add(colummname, item);
                }
                string sqlinsert = "";
                foreach (SuperItem item in Items.Values)
                {
                    sqlinsert = sqlinsert + string.Format("insert into " + tablename + " (StartTime,EndTime,City,Station,Item,Value,Quality,Type,DValue) values('{0}','{1}','{2}','{3}',{4},{5},'{6}','{7}',{8});", start, end, city, station, item.Item, item.Value, item.Quality, type, item.Value);
                }
                m_Database.Execute(sqlinsert);
            }
            /// <summary>
            /// 更外部数据
            /// </summary>
            /// <param name="row"></param>
            /// <param name="station"></param>
            /// <param name="type"></param>
            public static void UpdateVarDb(DataTable sources, string station, string type, DateTime time, string tablename)
            {
                Dictionary<string, SuperItem> Items = new Dictionary<string, SuperItem>();

                DataTable Table_Item = m_Database.GetDataset("select * from D_ItemCode").Tables[0];
                DataTable Table_Parameter = m_Database.GetDataset("select * from [parameter]").Tables[0];
                int maxdm = Convert.ToInt32(m_Database.GetFirstValue("select MAX(DM) from  D_ItemCode"));
                foreach (DataRow row in sources.Rows)
                {
                    double value = Convert.ToDouble(row["value"].ToString());
                    int parameterid = Convert.ToInt32(row["parameterid"].ToString());
                    string opcode = row["opcode"].ToString();
                    int itemcode = -1;
                    if (value == 999)
                        value = -99;
                    string mc = Table_Parameter.Select("parameterid = " + parameterid + "")[0]["name"].ToString();
                    try
                    {

                        itemcode = int.Parse(Table_Item.Select("MC = '" + mc + "'")[0]["DM"].ToString());
                    }
                    catch
                    {
                        maxdm = maxdm + 1;
                        string insertsql = " insert into D_ItemCode ([DM],[MC]  ,[Type])  values (" + maxdm + ",'" + mc + "','" + type + "') ";
                        m_Database.Execute(insertsql);
                        itemcode = maxdm;
                    }
                    SuperItem item = new SuperItem("", value, itemcode, opcode);
                    Items.Add(mc, item);
                }
                string sqlinsert = "";
                foreach (SuperItem item in Items.Values)
                {
                    sqlinsert = sqlinsert + string.Format("insert into {10} (StartTime,EndTime,City,Station,Item,Value,Quality,Type,OPCode,DValue) values('{0}','{1}','{2}','{3}',{4},{5},'{6}','{7}','{8}',{9});", time, time.AddHours(1), "上海", station, item.Item, item.Value, item.Quality, type, item.Opcode, item.Value, tablename);
                }
                m_Database.Execute(sqlinsert);
            }

        }

        private class SuperItem
        {
            public string Quality;
            public double Value;
            public int Item;
            public string Opcode;
            public SuperItem(string quality, double value, int item, string opcode)
            {
                // TODO: Complete member initialization
                Quality = quality;
                Value = value;
                Item = item;
                Opcode = opcode;
            }

        }
        /// <summary>
        /// 源类
        /// </summary>
        private enum TypeCode
        {
            建筑涂料使用 = 7,
            民用燃烧源 = 9,
            餐饮油烟 = 0,
            建筑施工 = 6,
            农业机械 = 10,
            污水处理 = 11,
            垃圾填埋 = 12,
            干洗 = 14,
            家用溶剂 = 13,
            油气挥发 = 15,
            道路扬尘 = 8,
            畜禽养殖=16,
            氮肥施用=22,
            土壤本底 = 23,
            人体粪便 = 24,
        };
        /// <summary>
        /// 枚举污染物编号
        /// </summary>
        private enum ITEM
        {
            Sun = 1,
            Zhuoduji = 2,
            Capsalb = 3,
            Jvalues = 4,
            O3Leida=5,
        };

        private DateTime CorrectTime(DateTime inputTime,int interval)
        {
            DateTime stime = DateTime.Parse(inputTime.ToString("yyyy-MM-dd HH:00:00"));
            int minute = int.Parse(inputTime.Minute.ToString());
            //取上面和下面
            int up = 0, down = 0;
            bool falg = false;
            for (int i = 0; i <= 60; i++)
            {
                if (falg) break;
                if (minute < i)
                {
                    for (int j = interval; j <= 60; j += interval)
                    {
                        if (i % j == 0)
                        {
                            up = i;
                            down = i - interval;
                            falg = true;
                            break;
                        }
                    }
                }
            }

            DateTime upTime = stime.AddMinutes(up);
            DateTime downTime = stime.AddMinutes(down);
            //时间就近
            TimeSpan ts1 = upTime - inputTime;
            TimeSpan ts2 = inputTime - downTime;
            if (ts1 > ts2)
            {
                stime = downTime;
            }
            else if (ts1 < ts2)
            {
                stime = upTime;
            }
            return stime;
        }
        public int GetCityCode(string city, string province)
        {
            try
            {
                int code = Convert.ToInt32(m_Database.GetFirstValue("SELECT  [Code]  FROM [D_SourceCity] where  CityName='" + city + "' and  Province='" + province + "'"));
                return code;
            }
            catch { return -1; }
        }
        public int GetParameterID(string items)
        {
            try
            {
                int code = Convert.ToInt32(m_Database.GetFirstValue("SELECT  DM  FROM [D_SourceLevelPar] where  Parameter='" + items + "'"));
                return code;
            }
            catch
            {
                int maxcode = Convert.ToInt32(m_Database.GetFirstValue("SELECT max( DM)  FROM [D_SourceLevelPar]"));
                string insetsql = "insert into D_SourceLevelPar values (" + ++maxcode + ",'" + items + "')";
                m_Database.Execute(insetsql);
                return maxcode;
            }
        }

        public static DataTable ExcelToDS(string Path, string SheetName)
        {
            string strConn = "Provider=Microsoft.ACE.OLEDB.12.0;" + "Data Source=" + Path + ";" + ";Extended Properties=\"Excel 12.0;HDR=YES;IMEX=1\"";
            OleDbConnection conn = new OleDbConnection(strConn);
            conn.Open();
            string strExcel = "";
            OleDbDataAdapter myCommand = null;
            DataTable ds = new DataTable();
            strExcel = "select * from " + SheetName;
            myCommand = new OleDbDataAdapter(strExcel, strConn);
            myCommand.Fill(ds);
            return ds;
        }
        public static DataTable ToCSV(string csvpath, char sp)
        {
            // string pCsvPath = "D:/auto.csv";//文件路径
            try
            {
                String line;
                String[] split = null;
                DataTable table = new DataTable();
                DataRow row = null;
                StreamReader sr = new StreamReader(csvpath, System.Text.Encoding.UTF8);
                //创建与数据源对应的数据列 
                line = sr.ReadLine();
                split = line.Split(sp);
                foreach (String colname in split)
                {
                    table.Columns.Add(colname, System.Type.GetType("System.String"));
                }
                //将数据填入数据表 
                int j = 0;
                while ((line = sr.ReadLine()) != null)
                {
                    j = 0;
                    row = table.NewRow();
                    split = line.Split(sp);
                    foreach (String colname in split)
                    {
                        row[j] = colname;
                        j++;
                    }
                    table.Rows.Add(row);
                }
                sr.Close();
                return table;
                //显示数据 
                //this.dataGridView1.DataSource = table.DefaultView;
            }
            catch (Exception vErr)
            {
                return null;
            }
        }
        
    }
}

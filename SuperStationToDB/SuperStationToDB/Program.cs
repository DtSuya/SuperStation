using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ChinaAirUtility;
using System.Threading;
using Readearth.Data;
using System.IO;
using System.Data;
using System.Data.OleDb;
using Utility.SuperStation;

namespace SuperStationToDB
{
    class Program
    {

        static string ConnectionStrings = "";
        static Database m_Database;
        static int Main(string[] args)
        {
            try
            {
                args = new string[] { "{\"LST\":\"2017/03/17 00:59:00\",\"Type\":\"外表入库\"}", "{\"ConnectionString\":\"Data Source=211.144.122.59,3433;Initial Catalog=SEMCShare;Persist Security Info=True;User ID=sa;Password=diting\",\"TableName\":\"T_SourceAclevel\",\"FilePath\":\"D:/SMMCDatabase/ModuleTest/DataSource/OMI/2017/20170201_104_DWD-MOHP.csv\",\"CreateTaskType\":\"MosaicLidar1\"}", "{}" };

                ResolvePar resolvePar = new ResolvePar(args);
                ConnectionStrings = resolvePar.TryGetValue("ConnectionString");
                m_Database = new Database(ConnectionStrings);
                string type = resolvePar.TryGetValue("Type");
                DateTime LST = Convert.ToDateTime(resolvePar.TryGetValue("LST"));
                string tablename = resolvePar.TryGetValue("TableName");
                string filename = resolvePar.TryGetValue("FilePath");
                string station = "环科院站";
                string city = "上海";
                ScheduleTask m_ScheduleTask = new ScheduleTask(ConnectionStrings);
                if (type == "颗粒" )
                {
                    DataTable Table = ToCSV(filename, ',');
                    foreach (DataRow row in Table.Rows)
                    {
                        DateTime starttime = Convert.ToDateTime(row[0]);
                        UpdateVarDb(row, station, type, city);
                    }
                }
                else if (type == "Hmetal")
                {
                    
                    string csvpath = resolvePar.TryGetValue("DataBasePath") + "SourceData/Hmetal/" + LST.ToString("yyyy") + "/" + LST.ToString("yyyyMMdd") + "/" + LST.ToString("yyyyMMddHHmm") + ".txt";
                    if (File.Exists(csvpath))
                    {
                        if (LST.Minute > 30)
                            LST = Convert.ToDateTime(LST.ToString("yyyy-MM-dd HH:00:00")).AddHours(1);
                        else
                            LST = Convert.ToDateTime(LST.ToString("yyyy-MM-dd HH:00:00"));
                        UpdateDb(ToCSV(csvpath, ','), type, LST, city, station);
                    }
                }
                else if (type == "OMI")
                {
                    ExternalData.m_Database = m_Database;
                    DataTable sources = ExternalData.MonthColumnO3toDb(filename, type, station, city);
                    foreach (DataRow row in sources.Rows)
                    {
                        DateTime starttime = Convert.ToDateTime(row[0]);
                        UpdateVarDb(row, station, type, city);
                    }

                }
                else if (type == "外表入库")
                {

                    foreach (int itemid in Enum.GetValues(typeof(ITEM)))
                    {
                        string temptype = ((ITEM)itemid).ToString();
                        DateTime minmaxtime = Convert.ToDateTime(m_Database.GetFirstValue("select max(lst) from  [ssta].[dbo].[" + temptype + "1]"));
                        string strminlasttime = m_Database.GetFirstValue("select max(StartTime) from  T_SuperStationmin where  Type='" + temptype + "'");
                        if (temptype == "O3Leida")
                        {
                            strminlasttime = m_Database.GetFirstValue("select max(LST) from  T_Lidar where  Type like '" + temptype + "%'");

                        }
                        string strlasttime = m_Database.GetFirstValue("select max(StartTime) from  T_SuperStation where  Type='" + temptype + "'");
                        DateTime minlasttime = minmaxtime.AddHours(-72);
                        DateTime lasttime = Convert.ToDateTime(minmaxtime.AddHours(-72).ToString("yyyy-MM-dd HH:00:00"));
                        if (strminlasttime != "")
                            minlasttime = Convert.ToDateTime(strminlasttime);
                        if (strlasttime != "")
                            lasttime = Convert.ToDateTime(strlasttime);

                        ///////////
                        minlasttime = Convert.ToDateTime("2017/01/01 00:00:00");
                        minmaxtime = Convert.ToDateTime("2017/01/31 00:00:00");
                        DataTable timelist = m_Database.GetDataset(string.Format("select distinct lst  from [ssta].[dbo].[{0}1] where  lst between '{1}' and  '{2}' order by  lst", temptype, minlasttime, minmaxtime)).Tables[0];
                        for (int i = 1; i < timelist.Rows.Count; i++)
                        {
                            DateTime time = Convert.ToDateTime(timelist.Rows[i][0]);
                            DataTable sources = m_Database.GetDataset(string.Format("select [parameterid] ,[value]  ,[opcode]  from [ssta].[dbo].[{0}1] where  lst='{1}'", temptype, time)).Tables[0];
                            try
                            {
                                if (temptype == "O3Leida")
                                {
                                    time = CorrectTime(time, 20);
                                    sources = ExternalData.O3Lidar(sources, time, temptype);
                                    InsertLidarToDB(sources, temptype, time);
                                    string CreateTaskType = resolvePar.TryGetValue("CreateTaskType");

                                    if (CreateTaskType != null && time.Minute == 0)
                                    {
                                        var taskPro = m_ScheduleTask.taskPros.FirstOrDefault(p => p.Name == CreateTaskType);
                                        CreateTask.InsertTask(time, taskPro);
                                    }
                                }
                                else
                                {
                                    UpdateVarDb(sources, station, temptype, time, "T_SuperStationmin");
                                    if (temptype == "Sun")
                                    {
                                        double v = 121.43561;
                                        double latitude = 31.17599;
                                        ExternalData.m_Database = m_Database;
                                        sources = ExternalData.CalAOD(v, latitude, time, sources);
                                        UpdateVarDb(sources, station, temptype, time, "T_SuperStation");
                                    }
                                }

                            }
                            catch { }
                            if (temptype == "Zhuoduji" || temptype == "Capsalb" || temptype == "Jvalues")
                                for (DateTime dtime = lasttime.AddHours(1); dtime < Convert.ToDateTime(time.ToString("yyyy-MM-dd HH:00:00")); dtime = dtime.AddHours(1))
                                {
                                    string Str_SQL = "select '" + dtime + "' as [StartTime]  ,'" + dtime.AddHours(1) + "' as [EndTime]  ,[City]  ,[Station]  ,[Item] ,avg([Value]) as [Value]  ,'' as [Quality]  ,[Type]  ,avg([DValue]) as [DValue]  ,'' as [OPCode] ,'' as [LMark]  ,'' as [HMark] ,'' as [State] from T_SuperStationmin where convert(varchar(13),StartTime,120)+':00:00'='" + dtime.ToString("yyyy-MM-dd HH:00:00") + "'  and   Type='" + temptype + "'  and  [DValue] !=-99 and  [Value]!=-99 group by  [City]  ,[Station], [Type],[Item] ";

                                    DataTable Table_Data = m_Database.GetDataset(Str_SQL).Tables[0];
                                    if (Table_Data.Rows.Count != 0)
                                        UpdateDb(Table_Data, type, dtime, city, station);
                                    lasttime = dtime;
                                }
                        }
                    }
                }
                else
                {
                    try
                    {
                        //string filename = resolvePar.TryGetValue("FilePath");
                        string insertsql = "insert into " + tablename + " ([Year]  ,[Code]  ,[ParameterID]  ,[TypeCode]   ,[CLType]  ,[EcoCode]  ,[Value]) values ";
                        DataTable Table = ToCSV(filename, ',');
                        int typecode = (int)Enum.Parse(typeof(TypeCode), type);
                        int i = 0;
                        foreach (DataRow row in Table.Rows)
                        {
                            int year = Convert.ToInt32(row[0]);
                            city = row[1].ToString();
                            string province = row[2].ToString();
                            int code = GetCode(city, province);
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
                                catch { }
                                int parameterid = GetParameterID(Table.Columns[j].ColumnName);
                                insertsql += string.Format("({0},{1},{2},{3},{4},{5},{6}),", year, code, parameterid, typecode, cltype, ecocode, value);
                                i++;
                                if (i > 900)
                                {
                                    m_Database.Execute(insertsql.Remove(insertsql.Length - 1));
                                    insertsql = "insert into " + tablename + " ([Year]  ,[Code]  ,[ParameterID]  ,[TypeCode]   ,[CLType]  ,[EcoCode]  ,[Value]) values ";
                                    i = 0;
                                }
                            }
                        }
                        m_Database.Execute(insertsql.Remove(insertsql.Length - 1));
                    }
                    catch { }
                }
                return 2;
            }
            catch { return -1; }
        }

        public static int GetCode(string city, string province)
        {
            try
            {
                int code = Convert.ToInt32(m_Database.GetFirstValue("SELECT  [Code]  FROM [D_SourceCity] where  CityName='" + city + "' and  Province='" + province + "'"));
                return code;
            }
            catch { return -1; }
        }
        public static int GetParameterID(string items)
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
        private static DateTime CorrectTime(DateTime inputTime,int interval)
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
        class SuperItem
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
        enum TypeCode
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
        enum ITEM
        {
            Sun = 1,
            //Zhuoduji = 2,
            Capsalb = 3,
            Jvalues = 4,
            O3Leida=5,
        };
        /// <summary>
        /// 超级站同表入库
        /// </summary>
        /// <param name="result"></param>
        /// <param name="type"></param>
        /// <param name="lst"></param>
        /// <param name="city"></param>
        /// <param name="station"></param>
        private static void UpdateDb(DataTable result, string type, DateTime lst, string city, string station)
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
        private static void InsertLidarToDB(DataTable result, string type, DateTime lst)
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
        private static void UpdateVarDb(DataRow row, string station, string type,string  city )
        {
            DateTime start = Convert.ToDateTime(row[0]);
            DateTime end = Convert.ToDateTime(row[0]);
            int superstationCount =
                           Convert.ToInt32(m_Database.GetFirstValue(
                               "if exists (SELECT TOP 1 1 FROM T_SuperStation where StartTime = '" + start +
                               "' and  Type ='" + type + "' and Station='" + station + "' ) SELECT 1 ELSE SELECT 0"));
            if (superstationCount != 0)
                m_Database.Execute("delete T_SuperStation where StartTime = '" + start +
                    "' and  Type ='" + type + "' and Station='" + station + "'");
            Dictionary<string, SuperItem> Items = new Dictionary<string, SuperItem>();
            
            DataTable Table_Item = m_Database.GetDataset("select * from D_ItemCode").Tables[0];
            int maxdm = Convert.ToInt32(m_Database.GetFirstValue("  select MAX(DM) from  D_ItemCode"));

            for (int i = 1; i < row.Table.Columns.Count; i++)
            {
                string colummname = row.Table.Columns[i].ColumnName;
                double value = Convert.ToDouble(row[i].ToString() != "" ? row[i].ToString() : "999");
                if (colummname.Contains("Column"))
                    continue;
                int itemcode = -1;
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
                sqlinsert = sqlinsert + string.Format("insert into T_SuperStation (StartTime,EndTime,City,Station,Item,Value,Quality,Type,DValue) values('{0}','{1}','{2}','{3}',{4},{5},'{6}','{7}',{8});", start, end, city, station, item.Item, item.Value, item.Quality, type, item.Value);
            }
            m_Database.Execute(sqlinsert);
        }
        /// <summary>
        /// 更外部数据
        /// </summary>
        /// <param name="row"></param>
        /// <param name="station"></param>
        /// <param name="type"></param>
        private static void UpdateVarDb(DataTable sources, string station, string type, DateTime time,string tablename)
        {
            Dictionary<string, SuperItem> Items = new Dictionary<string, SuperItem>();

            DataTable Table_Item = m_Database.GetDataset("select * from D_ItemCode").Tables[0];
            DataTable Table_Parameter = m_Database.GetDataset("select * from [parameter]").Tables[0];
            int maxdm = Convert.ToInt32(m_Database.GetFirstValue("  select MAX(DM) from  D_ItemCode"));
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
            m_Database.Execute(string.Format("delete {0} where StartTime='{1}' and  Type='{2}'", tablename, time, type));
            m_Database.Execute(sqlinsert);
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
                StreamReader sr = new StreamReader(csvpath, System.Text.Encoding.Default);
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

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
        string type;
        DataTable Table_Item;
        DateTime InitialLST { get; set; }

        public string Type 
        {
            get
            {
                return type;
            }
            set 
            {
                type = value;

                //原始在update中，每次更新时获得
                if(value == "VOCs")
                    Table_Item = m_Database.GetDataTable("select * from D_VOCItem");
                else
                    Table_Item = m_Database.GetDataTable("select * from D_ItemCode");
            }
        }

        public SuperStation(string connectionstrings) {
            ConnectionStrings = connectionstrings;
            m_Database = new Database(ConnectionStrings);
        }

        public int InsertToDB(string filepath)
        {
            try
            {
                if (Type == "VOCs" || Type == "颗粒")
                {
                    DataTable Table = ToCSV(filepath, ',');
                    foreach (DataRow row in Table.Rows)
                    {
                        UpdateVarDb(row, "T_SuperStation");
                    }
                }
                else if (Type == "OMI")
                {
                    O3Lidar O3Leida = new O3Lidar(m_Database);
                    DataTable Table = O3Leida.MonthColumnO3toDb(filepath, Type, Station, City);
                    foreach (DataRow row in Table.Rows)
                    {
                        UpdateVarDb(row, "T_SuperStation");
                    }
                }
                else
                {
                    LogManager.WriteLog(LogFile.Warning, "未知数据源：" + Type);
                    return -1;
                }
                return 2;
            }
            catch (Exception ex)
            {
                LogManager.WriteLog(ex);
                return -1; 
            }
        }

        public int InsertToDBHmetal(DateTime LST, string filepath)
        {
            try
            {
                //if (Type == "Hmetal")
                //{
                    string file = filepath + LST.ToString("yyyy") + "/" + LST.ToString("yyyyMMdd") + "/" + LST.ToString("yyyyMMddHHmm") + ".txt";
                    if (File.Exists(file))
                    {
                        if (LST.Minute > 30)
                            InitialLST = Convert.ToDateTime(LST.ToString("yyyy-MM-dd HH:00:00")).AddHours(1);
                        else
                            InitialLST = Convert.ToDateTime(LST.ToString("yyyy-MM-dd HH:00:00"));
                        DataTable Table = ToCSV(file, ',');
                        UpdateDb(Table, Type, InitialLST, City, Station);
                    }
                //}
                return 2;
            }
            catch (Exception ex)
            {
                LogManager.WriteLog(ex);
                return -1;
            }
        }

        public int InsertToDBMin(DateTime LST, string filepath)
        {
            try
            {
                InitialLST = LST.AddHours(-72);
                InitialLST = GetInitialStarttime("T_SuperStationmin");
                DataTable minsource;
                MinuteSource min = new MinuteSource();
                string singleColname;
                if (Type == "BC")
                {
                    singleColname = "IR_880nm";
                }
                else if (Type == "ScatCo")
                {
                    singleColname = "SP2";//σsp-525nm
                }
                else
                {
                    LogManager.WriteLog(LogFile.Warning, "未知数据源：" + Type);
                    return -1;
                }
                minsource = min.GetMinsource(Type, filepath, InitialLST, singleColname);

                if (minsource != null)
                    foreach (DataRow row in minsource.Rows)
                    {
                        //分钟数据入库
                        UpdateVarDb(row, "T_SuperStationmin");
                        //小时数据入库
                        InitialLST = GetInitialStarttime("T_SuperStation");
                        DateTime minutime = Convert.ToDateTime(row[0].ToString());
                        for (DateTime stime = InitialLST; stime < Convert.ToDateTime(minutime.ToString("yyyy-MM-dd HH:00:00"));  
                            stime = stime.AddHours(1))
                        {
                            string Str_SQL = string.Format(@"select '{0}' as [StartTime],  '{1}' as [EndTime],  [City],  [Station]  ,[Item], 
                                avg([Value]) as [Value],  '' as [Quality],  [Type],  avg([DValue]) as [DValue],  '' as [OPCode],  '' as [LMark],  '' as [HMark], '' as [State] 
                                from T_SuperStationmin where convert(varchar(13),StartTime,120)+':00:00'='{2}'  and  Type='{3}'  and  [DValue] !=-99 and  [Value]!=-99 
                                group by  [City]  ,[Station], [Type],[Item] ",
                                stime, stime.AddHours(1),stime.ToString("yyyy-MM-dd HH:00:00"), Type);
                            DataTable Table_hour = m_Database.GetDataTable(Str_SQL);
                            if (Table_hour.Rows.Count != 0)
                                UpdateDb(Table_hour, Type, stime, City, Station);
                        }
                    }
                return 2;
            }
            catch (Exception ex)
            {
                LogManager.WriteLog(ex);
                return -1;
            }
        }

        public int InsertToDBHour(DateTime LST, string filepath)
        {
            try
            {
                InitialLST = LST.AddHours(-72);
                InitialLST = GetInitialStarttime("T_SuperStation");
                DataTable Table;
                if (Type == "OCEC")
                {
                    OCEC ocec = new OCEC();
                    Table = ocec.GetOCECSource(filepath, InitialLST);
                }
                else if (Type == "Spectra")
                {
                    Spectra SPAMS = new Spectra();
                    Table = SPAMS.GetSPAMSsource(filepath, InitialLST);
                }
                else
                {
                    LogManager.WriteLog(LogFile.Warning, "未知数据源：" + Type);
                    return -1;
                }
                if(Table != null)
                    foreach (DataRow row in Table.Rows)
                    {
                        UpdateVarDb(row, "T_SuperStation");
                    }
                return 2;
            }
            catch (Exception ex)
            {
                LogManager.WriteLog(ex);
                return -1;
            }
        }

        #region 同表Update
        /// <summary>
        /// 超级站同表入库
        /// </summary>
        /// <param name="result"></param>
        /// <param name="type"></param>
        /// <param name="lst"></param>
        /// <param name="city"></param>
        /// <param name="station"></param>
        private void UpdateDb(DataTable result, string type, DateTime lst, string city, string station)
        {
            try
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
                    catch (Exception ex)
                    {
                        LogManager.WriteLog("超级站同表入库", ex);
                    }
                }
                m_Database.Execute(sqlinsert);
            }
            catch (Exception ex)
            {
                LogManager.WriteLog("超级站同表入库", ex);
            }
        }
        /// <summary>
        /// 雷达同表入库
        /// </summary>
        /// <param name="result"></param>
        /// <param name="type"></param>
        /// <param name="lst"></param>
        private void InsertLidarToDB(DataTable result, string type, DateTime lst)
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
            catch (Exception ex)
            {
                LogManager.WriteLog("雷达同表入库", ex);
            }
        }
        #endregion

        #region 外表Update
        /// <summary>
        /// 更外部数据(单列时段数据)
        /// </summary>
        /// <param name="result"></param>
        /// <param name="type"></param>
        /// <param name="lst"></param>
        /// <param name="city"></param>
        /// <param name="station"></param>
        /// <param name="tablename"></param>
        private void UpdateVarDb(DataTable result, DateTime lst, string tablename)
        {
            try
            {
                Dictionary<string, SuperItem> Items = new Dictionary<string, SuperItem>();

                DataTable Table_Parameter = m_Database.GetDataTable("select * from [parameter]");
                int maxdm = Convert.ToInt32(m_Database.GetFirstValue("select MAX(DM) from  D_ItemCode"));

                int superstationCount = Convert.ToInt32(m_Database.GetFirstValue("if exists (SELECT TOP 1 1 FROM " + tablename + " where StartTime = '" + lst + "' and  Type ='" + Type + "' and City ='" + City + "' and Station='" + Station + "' ) SELECT 1 ELSE SELECT 0"));
                if (superstationCount != 0)
                    m_Database.Execute("delete " + tablename + " where StartTime = '" + lst + "' and  Type ='" + Type + "' and City ='" + City + "' and Station='" + Station + "'");
                foreach (DataRow row in result.Rows)
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
                        string insertsql = " insert into D_ItemCode ([DM], [MC], [Type])  values (" + maxdm + ",'" + mc + "','" + Type + "') ";
                        m_Database.Execute(insertsql);
                        itemcode = maxdm;
                        LogManager.WriteLog(LogFile.SQL, "插入新的ItemCode：" + itemcode + ", MC：" + mc);

                        if (Type == "VOCs")
                            Table_Item = m_Database.GetDataTable("select * from D_VOCItem");
                        else
                            Table_Item = m_Database.GetDataTable("select * from D_ItemCode");
                    }
                    SuperItem item = new SuperItem("", value, itemcode, opcode);
                    Items.Add(mc, item);
                }
                string sqlinsert = "";
                foreach (SuperItem item in Items.Values)
                {
                    sqlinsert = sqlinsert + string.Format("insert into {10} (StartTime,EndTime,City,Station,Item,Value,Quality,Type,OPCode,DValue) values('{0}','{1}','{2}','{3}',{4},{5},'{6}','{7}','{8}',{9});", lst, lst.AddHours(1), City, Station, item.Item, item.Value, item.Quality, Type, item.Opcode, item.Value, tablename);
                }
                m_Database.Execute(sqlinsert);
            }
            catch (Exception ex)
            {
                LogManager.WriteLog("超级站外部(时段)数据入库：Type ="+ Type +", StartTime ="+ lst , ex);
            }
        }
        /// <summary>
        /// 更新外部数据(多列时刻数据)（原：“更颗粒/VOC”合并）
        /// </summary>
        /// <param name="row"></param>
        /// <param name="type"></param>
        /// <param name="city"></param>
        /// <param name="station"></param>
        /// <param name="tablename"></param>
        private void UpdateVarDb(DataRow row, string tablename)
        {
            try
            {
                DateTime start = Convert.ToDateTime(row[0].ToString());
                DateTime end = Convert.ToDateTime(row[0].ToString());
                Dictionary<string, SuperItem> Items = new Dictionary<string, SuperItem>();
                
                int maxdm = Convert.ToInt32(m_Database.GetFirstValue("select MAX(DM) from  D_ItemCode"));

                int superstationCount = Convert.ToInt32(m_Database.GetFirstValue("if exists (SELECT TOP 1 1 FROM " + tablename + " where StartTime = '" + start + "' and  Type ='" + Type + "' and City ='" + City + "' and Station='" + Station + "' ) SELECT 1 ELSE SELECT 0"));
                if (superstationCount != 0)
                    m_Database.Execute("delete " + tablename + " where StartTime = '" + start + "' and  Type ='" + Type + "' and City ='" + City + "' and Station='" + Station + "'");

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
                        if (Type == "VOCs")
                            itemcode = int.Parse(this.Table_Item.Select("Name = '" + colummname + "'")[0]["Item"].ToString());
                        else
                            itemcode = int.Parse(Table_Item.Select("Type = '"+ Type +"' and MC = '" + colummname + "'")[0]["DM"].ToString());
                    }
                    catch
                    {
                        maxdm = maxdm + 1;
                        string insertsql = " insert into D_ItemCode ([DM], [MC] ,[Type])  values (" + maxdm + ",'" + colummname + "','" + Type + "')";
                        if (Type == "VOCs" || Type == "颗粒")
                            insertsql += "; insert into D_ModelAnalysis_Item ([DM], [MC] ,[DP])  values (" + maxdm + ",'" + colummname + "','" + Type + "')";
                        m_Database.Execute(insertsql);
                        itemcode = maxdm;
                        LogManager.WriteLog(LogFile.SQL, "插入新的ItemCode：" + itemcode + ", MC：" + colummname);

                        if (Type == "VOCs")
                            Table_Item = m_Database.GetDataTable("select * from D_VOCItem");
                        else
                            Table_Item = m_Database.GetDataTable("select * from D_ItemCode");
                    }
                    SuperItem item = new SuperItem("", value, itemcode, "");
                    Items.Add(colummname, item);
                }
                string sqlinsert = "";
                foreach (SuperItem item in Items.Values)
                {
                    sqlinsert = sqlinsert + string.Format("insert into " + tablename + " (StartTime,EndTime,City,Station,Item,Value,Quality,Type,DValue) values('{0}','{1}','{2}','{3}',{4},{5},'{6}','{7}',{8});", start, end, City, Station, item.Item, item.Value, item.Quality, Type, item.Value);
                }
                m_Database.Execute(sqlinsert);
            }
            catch(Exception ex)
            {
                LogManager.WriteLog("超级站外部(多列)数据入库：Type =" + Type, ex);
            }
        }
        #endregion

        //获取类型最后更新时间
        private DateTime GetInitialStarttime(string tabelname)
        {
            string strlasttime =
                            m_Database.GetFirstValue("select max(StartTime) from  " + tabelname + " where  Type='" + Type + "'");
            DateTime lasttime;
            if (DateTime.TryParse(strlasttime, out lasttime))
                return lasttime;
            else
                return InitialLST;
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
                LogManager.WriteLog(vErr);
                return null;
            }
        }
        
    }
}

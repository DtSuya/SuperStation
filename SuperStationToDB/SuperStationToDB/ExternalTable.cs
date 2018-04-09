using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using Readearth.Data;
using System.IO;

namespace Utility.SuperStation
{
    class ExternalTable
    {
        static Database m_Database;
        
        public class Sun
        {
            static double m_P;//观测点压强
            static double m_U;//臭氧含量
            static double m_v;//观测点经度
            static double m_Theta;//太阳天顶角
            static double m_Latitude;//观测点纬度
            public Sun(Database Database)
            {
                m_Database = Database;

            }
            /// <summary>
            /// 计算各波段AOD值并返回结果
            /// </summary>
            /// <param name="P">观测点压强，单位为HPa</param>
            /// <param name="U">臭氧含量，单位为DU</param>
            /// <param name="v">观测点经度，单位为度</param>
            /// <param name="Latitude">观测点纬度，单位为度</param>
            /// <param name="time">观测时间</param>
            /// <param name="sources">某一时刻的各波段DN测量值，包含parameterid,value,opcode字段</param>
            public DataTable CalAOD(double v, double Latitude, DateTime time, DataTable sources)
            {
                DataTable dtAOD = new DataTable();
                dtAOD.Columns.Add("parameterid", typeof(Int32));
                dtAOD.Columns.Add("value", typeof(double));
                dtAOD.Columns.Add("opcode", typeof(string));
                try
                {
                    string tablename = "D_AODCode";
                    DataTable waveTable = m_Database.GetDataset("select * from " + tablename).Tables[0];

                    m_v = v;
                    m_Latitude = Latitude;
                    //获取压强数据
                    try
                    {
                        DateTime hourtime = time.Date.AddHours(time.Hour);
                        m_P = Double.Parse(m_Database.GetFirstValue("select Value from T_SuperStation where StartTime='" + time + "' and Item=28"));
                        m_Theta = Double.Parse(m_Database.GetFirstValue("select DValue from T_SuperStationmin where  CONVERT(varchar(100), StartTime, 23)='" + time.Date + "' and  DATEPART(hh,StartTime)=" + time.Hour + "   and Item=360"));
                    }
                    catch
                    {
                        return dtAOD;
                    }
                    //获取当天臭氧含量
                    try
                    {
                        m_U = Convert.ToDouble(m_Database.GetFirstValue("select DValue from T_SuperStation where where  CONVERT(varchar(100), StartTime, 23)='" + time.Date + "' and  DATEPART(hh,StartTime)=" + time.Hour + "   and  Item=359 AND [Type]='" + "OMI'"));
                    }
                    catch
                    {
                        m_U = 302.2000;
                    }
                    //440
                    DataRow drsource = sources.Select("parameterid = 114")[0];
                    double a_aod = SpecialAOD(440, Convert.ToDouble(drsource["value"]), time, waveTable);
                    //870
                    drsource = sources.Select("parameterid =112")[0];
                    double b_aod = SpecialAOD(870, Convert.ToDouble(drsource["value"]), time, waveTable);

                    double alpha = -Math.Log10(a_aod / b_aod) / Math.Log10(440.0 / 870.0);
                    double beta = a_aod / Math.Pow(440, -alpha);

                    foreach (DataRow i in sources.Rows)
                    {
                        if (i["parameterid"].ToString().Equals("120"))
                            continue;
                        DataRow dr = dtAOD.NewRow();
                        dr["parameterid"] = i["parameterid"];
                        dr["value"] = 0;
                        dr["opcode"] = i["opcode"];
                        double wave = 0;
                        wave = Convert.ToInt32(waveTable.Select("Parameterid = " + i["parameterid"])[0]["Wave"]);
                        if (wave == 440)
                            dr["value"] = a_aod;
                        else if (wave == 870)
                            dr["value"] = b_aod;
                        else
                        {
                            dr["value"] = beta * Math.Pow(wave, -alpha);
                        }
                        dtAOD.Rows.Add(dr);
                    }
                }
                catch
                {
                    dtAOD.Clear();
                    foreach (DataRow i in sources.Rows)
                    {
                        DataRow dr = dtAOD.NewRow();
                        dr["parameterid"] = i["parameterid"];
                        dr["value"] = -99;
                        dr["opcode"] = "H";
                        dtAOD.Rows.Add(dr);
                    }
                }
                return dtAOD;
            }
            /// <summary>
            /// 计算440和870两特殊波段的AOD值
            /// </summary>
            /// <param name="wave1">波段值(波长)</param>
            /// <param name="dn">DN观测值</param>
            /// <param name="LST">观测时间</param>
            /// <param name="WaveTable">字典数据</param>
            /// <returns>double类型的AOD值</returns>
            private static double SpecialAOD(int wave1, double dn, DateTime LST, DataTable WaveTable)
            {
                double aod = 0;
                try
                {
                    double wave = Convert.ToDouble(wave1);
                    double aod_r = 0.008569 * Math.Pow(wave, -4) * (1 + 0.0113 * Math.Pow(wave, -2) + 0.00013 * Math.Pow(wave, -4)) * m_P / 1013.25;

                    DataRow dr = WaveTable.Select("Wave = " + wave)[0];
                    double aod_oz = Convert.ToDouble(dr["rate"]) * m_U / 1000;

                    int dno = Convert.ToInt32(dr["DNO"]);
                    double[] a = { 1.000110, 0.034221, 0.0000719 };
                    double[] b = { 0, 0.001280, 0.000077 };
                    int day = LST.DayOfYear;
                    double t = 2 * Math.PI * day / 365;
                    double ds = 0;
                    for (int n = 0; n < 3; n++)
                    {
                        ds += a[n] * Math.Cos(n * t) + b[n] * Math.Sin(n * t);
                    }

                    double dila = (0.006918 - 0.399912 * Math.Cos(t) + 0.070257 * Math.Sin(t) - 0.006758 * Math.Cos(2 * t) + 0.000907 * Math.Sin(2 * t) - 0.002697 * Math.Cos(3 * t) + 0.00148 * Math.Sin(3 * t));//（弧度制）
                    double h = (LST.TimeOfDay.TotalHours + (m_v - 120) / 15 - 12) * 15;//（角度制）
                    double theta = Math.Acos(Math.Sin(m_Latitude * Math.PI / 180) * Math.Sin(dila) + Math.Cos(m_Latitude * Math.PI / 180) * Math.Cos(dila) * Math.Cos(h * Math.PI / 180));//（弧度制）
                    if (theta > Math.PI / 2)
                        theta = Math.PI - theta;
                    double m = 1 / (Math.Cos(theta) + 0.15 * Math.Pow(93.885 - theta * 180 / Math.PI, -1.253)) * m_P / 1013.25;

                    double aod_total = 1 / m * Math.Log10(dno * ds / dn);
                    aod = aod_total - aod_r - aod_oz;
                }
                catch { }
                return aod;
            }
        }

        public class O3Lidar
        {
            public O3Lidar(Database Database)
            {
                m_Database = Database;

            }
            /// <summary>
            /// 获取整月臭氧含量数据
            /// </summary>
            /// <param name="filepath">臭氧含量路径</param>
            /// <param name="type">Type</param>
            /// <param name="station">站点名称</param>
            /// <param name="city">"上海"</param>
            public DataTable MonthColumnO3toDb(string filepath, string type, string station, string city)
            {
                DataTable sources = new DataTable();
                sources.Columns.Add("StartTime", typeof(DateTime));
                sources.Columns.Add("O3", typeof(double));

                FileStream fs = new FileStream(filepath, FileMode.Open, FileAccess.Read);
                StreamReader sr = new StreamReader(fs, System.Text.Encoding.Default);
                string alldata = "";
                alldata = sr.ReadToEnd();
                string[] str;
                str = alldata.Split(new char[] { '\r', '\n' }, System.StringSplitOptions.RemoveEmptyEntries);
                int a = 0;
                int b = 0;
                for (int i = 0; i < str.Length; i++)
                {
                    string s = str[i];
                    if (s.Equals("#DAILY"))
                        a = i;
                    if (s.Equals("#MONTHLY"))
                        b = i;
                }
                for (int i = a + 2; i < b; i++)
                {
                    DateTime date = Convert.ToDateTime(str[i].Split(',')[0]);//Datetime

                    DataRow dr = sources.NewRow();
                    dr["StartTime"] = date;
                    dr["O3"] = Convert.ToDouble(str[i].Split(',')[3]);//ColumnO3

                    //判断是否存在重复记录
                    DataTable lastdata = m_Database.GetDataset(string.Format("select * from T_SuperStation where StartTime= '{0}' and type='{1}' ", date, type)).Tables[0];
                    if (lastdata.Rows.Count > 0)
                    {
                        continue;
                    }
                    sources.Rows.Add(dr);
                }
                fs.Close();
                sr.Dispose();
                return sources;
            }

            public static DataTable Lidar(DataTable o3leida1, DateTime lst, string type)
            {
                DataTable result = new DataTable();
                result.Columns.Add("Height", typeof(double));
                result.Columns.Add("Lidarvalue", typeof(double));

                result.Columns.Add("parameterid", typeof(int));
                Dictionary<string, string[]> Datas = new Dictionary<string, string[]>();
                int m_PerPicMinutes = 20;
                string tpDir = "D:\\SMMCDatabase\\Lidar\\";
                foreach (DataRow row in o3leida1.Rows)
                {
                    double height = 7.5;
                    float value = 0;
                    byte[] content = (byte[])row["value"];
                    string parameterid = row["parameterid"].ToString();
                    float[] tuipian = new float[content.Length / 4];
                    for (int i = 0; i < content.Length; i += 4, height += 7.5)
                    {
                        DataRow newrow = result.NewRow();
                        newrow["Height"] = height;
                        byte[] b4 = new byte[] { content[i], content[i + 1], content[i + 2], content[i + 3] };
                        value = BitConverter.ToSingle(b4, 0);//浮点型系数value
                        value = value == -999 ? 0 : value;
                        newrow["Lidarvalue"] = value;
                        newrow["parameterid"] = row["parameterid"];
                        result.Rows.Add(newrow);

                        tuipian[i / 4] = value;

                    }


                    string[] dnr = new string[content.Length / 4 / 2 + 1];

                    dnr[0] = lst.ToString("yyyy-MM-dd HH:mm:00");

                    for (int i = 0; i < tuipian.Length - 1; i = i + 2)
                    {

                        dnr[i / 2 + 1] = ((tuipian[i] + tuipian[i + 1]) / 2.0).ToString();

                    }
                    Datas.Add(type + parameterid, dnr);
                }
                foreach (KeyValuePair<string, string[]> key in Datas)
                {
                    //if (key.Key == "O3Leida192")
                    //    continue;
                    LidarCacheNew lidar = new LidarCacheNew(tpDir, m_PerPicMinutes);
                    lidar.Type = key.Key;
                    lidar.Lline = 1;
                    lidar.PixelsPerLine = 1;
                    lidar.TotalCount = 414;
                    if (key.Key == "O3Leida192")
                        lidar.Maxvalue = 200;
                    lidar.DoCreateNew(key.Value);
                }
                return result;

            }
        }
    }
    
}


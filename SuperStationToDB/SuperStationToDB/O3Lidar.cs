using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using Readearth.Data;
using System.IO;

namespace Utility.SuperStation
{
    class O3Lidar
    {
        Database m_Database;
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

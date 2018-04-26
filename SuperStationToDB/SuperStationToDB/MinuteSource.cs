using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data;
using Readearth.Data;

namespace Utility.SuperStation
{
    class MinuteSource
    {
        DataTable m_sourceTable;
        string m_Type;
        DateTime m_LST;
        string m_colname;

        public MinuteSource() 
        {  }

        public DataTable GetMinsource(string type, string folder, DateTime maxLST, string colName)
        {
            m_Type = type;
            m_LST = maxLST;
            m_colname = colName;
            m_sourceTable = new DataTable();
            m_sourceTable.Columns.Add("LST", typeof(string));
            m_sourceTable.Columns.Add(m_colname, typeof(double));

            if (type == "BC")
            {
                GetIR1source(ref m_sourceTable, folder);
            }
            else if (type == "ScatCo")
            {
                GetZhuoDYsource(ref m_sourceTable, folder);
            }
            else
                return null;
            if (m_sourceTable.Rows.Count > 0)
            {
                m_sourceTable.AcceptChanges();
                return m_sourceTable;
            }
            else
                return null;
        }

        //获取晚于LST的所有IR1数据
        public void GetIR1source(ref DataTable IR1, string folder)
        {
            string[] filenames = Directory.GetFiles(folder);
            foreach (string filename in filenames)
            {
                try
                {
                    DateTime fileTime = DateTime.ParseExact(filename.Substring(filename.LastIndexOf('.') - 6, 6), "yyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    DataTable table = GetHeiTXT(filename);
                    if(table != null)
                        foreach (DataRow row in table.Rows)
                        {
                            IR1.ImportRow(row);
                        }
                }
                catch (Exception ex)
                {
                    LogManager.WriteLog("黑炭数据解析", ex);
                }
            }
        }

        //获取当前黑炭文件中符合条件的数据
        private DataTable GetHeiTXT(string path)
        {
            try
            {
                DataTable table = m_sourceTable.Clone();
                DataRow row;
                System.Globalization.CultureInfo dateInfo = new System.Globalization.CultureInfo("en"); //26-feb-18 10:45

                string line;
                string[] split;
                StreamReader sr = new StreamReader(path, System.Text.Encoding.UTF8);

                while ((line = sr.ReadLine()) != null)
                {
                    row = table.NewRow();
                    split = line.Split(',');
                    DateTime drLST = Convert.ToDateTime(split[0].Trim('\0', '"') + " " + split[1].Trim('"'), dateInfo);
                    row["LST"] = drLST.ToString("yyyy-MM-dd HH:mm:00");
                    row[m_colname] = split[7];//"IR_880nm"
                    if (DateTime.Compare(m_LST, drLST) <= 0)
                        table.Rows.Add(row);
                }
                sr.Close();
                if (table.Rows.Count > 0)
                    return table;
                else
                    return null;
            }
            catch (Exception ex)
            {
                LogManager.WriteLog("黑炭数据解析：" + path, ex);
                return null;
            }
        }


        public void GetZhuoDYsource(ref DataTable ZDY, string folder)
        {
            try
            {
                string[] allyears = Directory.GetDirectories(folder);
                foreach (string direc in allyears)
                {
                    string yearfolder = direc.Split('/')[direc.Split('/').Length - 1];
                    DateTime year = DateTime.ParseExact(yearfolder, "yyyy", System.Globalization.CultureInfo.CurrentCulture);
                    if (year.Year >= m_LST.Year && Directory.Exists(folder + "/" + year.ToString("yyyy") + "/"))
                    {
                        string[] filenames = Directory.GetFiles(folder + "/" + year.ToString("yyyy") + "/");
                        foreach (string filefullname in filenames)
                        {
                            try
                            {
                                //Aurora3000 2017-12-26.txt
                                string filename = filefullname.Split('/')[filefullname.Split('/').Length - 1];
                                DateTime fileTime = DateTime.ParseExact(filename.Substring(11, 10), "yyyy-MM-dd", System.Globalization.CultureInfo.CurrentCulture);
                                if (fileTime > m_LST)
                                {
                                    DataTable table = GetZDYTXT(filefullname, "dd/MM/yyyy HH:mm:ss", 2);
                                    if(table != null)
                                        foreach (DataRow row in table.Rows)
                                        {
                                            ZDY.ImportRow(row);
                                        }
                                }
                            }
                            catch (Exception ex)
                            {
                                LogManager.WriteLog("浊度仪数据解析", ex);
                                continue;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogManager.WriteLog("浊度仪数据解析", ex);
            }
        }

        private DataTable GetZDYTXT(string path, string timeformat, int colindex)
        {
            try
            {
                DataTable table = m_sourceTable.Clone();
                DataRow row;
                string line;
                string[] split;
                StreamReader sr = new StreamReader(path, System.Text.Encoding.UTF8);

                while ((line = sr.ReadLine()) != null)
                {
                    if (!string.IsNullOrWhiteSpace(line))
                    {
                        row = table.NewRow();
                        split = line.Split(',');
                        DateTime drLST = DateTime.ParseExact(split[0], timeformat, System.Globalization.CultureInfo.CurrentCulture);

                        row["LST"] = drLST.ToString("yyyy-MM-dd HH:mm:00");
                        row[m_colname] = split[colindex];
                        table.Rows.Add(row);
                    }
                }
                sr.Close();
                if (table.Rows.Count > 0)
                    return table;
                else
                    return null;
            }
            catch (Exception ex)
            {
                LogManager.WriteLog("浊度仪数据解析：" + path, ex);
                return null;
            }
        }
    }
}

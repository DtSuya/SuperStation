using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Readearth.Data;
using System.IO;
using System.Data;
using System.Data.SqlClient;

namespace Utility.SuperStation
{
    class OCEC
    {
        DataTable m_sourceTable;
        DateTime m_LST;
        public OCEC()
        {  }

        public DataTable GetOCECSource(string folder, DateTime maxLST)
        {
            try
            {
                m_LST = maxLST;
                m_sourceTable = new DataTable();
                m_sourceTable.Columns.Add("LST", typeof(string));
                m_sourceTable.Columns.Add("OC", typeof(double));
                m_sourceTable.Columns.Add("EC", typeof(double));
                m_sourceTable.Columns.Add("TC", typeof(double));
                m_sourceTable.Columns.Add("OC/EC", typeof(double));
                m_sourceTable.Columns.Add("OC/TC", typeof(double));
                m_sourceTable.Columns.Add("EC/TC", typeof(double));

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
                                //SLI_OCEC_20180329_001146_Res.csv
                                string filename = filefullname.Split('/')[filefullname.Split('/').Length - 1];
                                DateTime fileTime = DateTime.ParseExact(filename.Substring(9, 8), "yyyyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                                if (fileTime >= m_LST.Date)
                                {
                                    DataTable table = GetOcecCSV(filefullname);
                                    if(table != null)
                                        foreach (DataRow row in table.Rows)
                                        {
                                            m_sourceTable.ImportRow(row);
                                        }
                                }

                            }
                            catch (Exception ex)
                            {
                                LogManager.WriteLog("在线OCEC数据解析", ex);
                                continue;
                            }
                        }
                    }
                }
                if (m_sourceTable.Rows.Count > 0)
                {
                    m_sourceTable.AcceptChanges();
                    return m_sourceTable;
                }
                else
                    return null;
            }
            catch (Exception ex)
            {
                LogManager.WriteLog("在线OCEC数据解析", ex);
                return null;
            }
        }

        private DataTable GetOcecCSV(string path)
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
                        if (split.Length > 1 && split[0] != "Sample ID")
                        {
                            //"3/29/2018 12:15:01 AM "注意tt后面有空格
                            DateTime drLST = DateTime.ParseExact(split[6], "M/d/yyyy h:mm:ss tt ", System.Globalization.CultureInfo.GetCultureInfo("en-US"));
                            row["LST"] = drLST.ToString("yyyy-MM-dd HH:00:00");
                            row["OC"] = split[3];
                            row["EC"] = split[4];
                            row["TC"] = split[5];
                            row["OC/EC"] = Convert.ToDouble(row["OC"]) / Convert.ToDouble(row["EC"]);
                            row["OC/TC"] = Convert.ToDouble(row["OC"]) / Convert.ToDouble(row["TC"]);
                            row["EC/TC"] = Convert.ToDouble(row["EC"]) / Convert.ToDouble(row["TC"]);
                            table.Rows.Add(row);
                        }
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
                LogManager.WriteLog("在线OCEC数据解析：" + path, ex);
                return null;
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.IO;

namespace Utility.SuperStation
{
    class Spectra
    {
        DateTime m_LST;
        public Spectra() { }

        public DataTable GetSPAMSsource(string folder, DateTime maxLST)
        {
            m_LST = maxLST;
            DataTable source;
            DataTable SPAMS = new DataTable();
            string[] filenames = Directory.GetFiles(folder);
            foreach (string filename in filenames)
            {
                try
                {
                    //SPAMS_2018-03-13-16-35-44.tst
                    string filetime = filename.Substring(filename.LastIndexOf('_') + 1, 13);
                    DateTime fileTime = DateTime.ParseExact(filetime, "yyyy-MM-dd-HH", System.Globalization.CultureInfo.CurrentCulture);
                    DataTable table = GetTST(filename,';');
                    if (SPAMS.Columns.Count < 1)
                        SPAMS = table.Copy();
                    else
                        foreach (DataRow row in table.Rows)
                        {
                            SPAMS.ImportRow(row);
                        }
                }
                catch (Exception ex)
                {
                    LogManager.WriteLog("单颗粒质谱数据解析", ex);
                    continue;
                }
                SPAMS.AcceptChanges();
            }
            if (SPAMS.Rows.Count > 0)
            {
                source = TrimRestCol(SPAMS);
                return source;
            }
            else
                return null;
        }

        //获取当前SPAMS文件中符合条件的数据
        private DataTable GetTST(string path, char sp)
        {
            try
            {
                String line;
                String[] split = null;
                DataTable table = new DataTable();
                DataRow row = null;
                StreamReader sr = new StreamReader(path, System.Text.Encoding.Default);
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
                    DateTime drLST = Convert.ToDateTime(split[0]);
                    if (DateTime.Compare(m_LST, drLST) <= 0)
                    {
                        foreach (String colname in split)
                        {
                            row[j] = colname;
                            j++;
                        }
                        table.Rows.Add(row);
                    }
                }
                sr.Close();
                return table;
            }
            catch (Exception ex)
            {
                LogManager.WriteLog("单颗粒质谱数据解析：" + path, ex);
                return null;
            }
        }

        //删除无效数据
        public DataTable TrimRestCol(DataTable sourceTable)
        {
            DataTable table = sourceTable.Copy();
            table.Columns.Remove("Mass");
            table.Columns.Remove("Hit");
            table.Columns.Remove("Size");
            table.Columns.Remove("经度");
            table.Columns.Remove("纬度");

            for (int i = 0; i < table.Rows.Count; i++)
            {
                if (i != table.Rows.Count - 1)
                {
                    //无数据或不为整时数据删除该行
                    DataRow row = table.Rows[i];
                    DateTime time = Convert.ToDateTime(row[0].ToString());
                    if (time.Minute > 0 || time.Second > 0)
                    {
                        table.Rows.Remove(row);
                        i--;
                    }
                    else
                    {
                        object[] values = row.ItemArray;
                        bool isempty = true;
                        for (int j = 1; j < values.Length;j++ )
                        {
                            try
                            {
                                double p = Convert.ToDouble(values[j]);
                                if (p != 0)
                                {
                                    isempty = false;
                                    break;
                                }
                            }
                            catch { table.Rows.Remove(row); i--; }
                        }
                        if (isempty)
                        {
                            table.Rows.Remove(row);
                            i--;
                        }
                    }
                }
                else
                {
                    //末行不为小时数据时取整时
                    DataRow lastrow = table.Rows[table.Rows.Count - 1];
                    DateTime etime = Convert.ToDateTime(lastrow[0].ToString());
                    if (etime.Minute > 0 || etime.Second > 0)
                    {
                        etime = Convert.ToDateTime(etime.ToString("yyyy/MM/dd HH:00:00")).AddHours(1);
                        lastrow[0] = etime.ToString("yyyy/MM/dd HH:00:00");
                    } 
                }
            }
            table.AcceptChanges();
            return table;
        }
    }
}

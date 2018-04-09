using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data;
using Readearth.Data;

namespace Utility.SuperStation
{
    class BC
    {
        static DateTime m_LST;
        public BC() { }

        //获取晚于LST的所有IR1数据
        public DataTable GetIR1source(string folder, DateTime maxLST)
        {
            m_LST = maxLST;
            DataTable IR1 = new DataTable();
            IR1.Columns.Add("LST", typeof(string));
            IR1.Columns.Add("IR_880nm", typeof(double));

            string[] filenames = Directory.GetFiles(folder);
            foreach (string filename in filenames)
            {
                try
                {
                    DateTime fileTime = DateTime.ParseExact(filename.Substring(filename.LastIndexOf('.') - 6, 6), "yyMMdd", System.Globalization.CultureInfo.CurrentCulture);
                    DataTable table = GetTXT(filename);
                    foreach (DataRow row in table.Rows)
                    {
                        IR1.ImportRow(row);
                    }
                }
                catch { }
            }
            return IR1;
        }

        //获取当前黑炭文件中符合条件的数据
        private DataTable GetTXT(string path)
        {
            try
            {
                DataTable table = new DataTable();
                table.Columns.Add("LST", typeof(string));
                table.Columns.Add("IR_880nm", typeof(double));
                DataRow row;
                System.Globalization.CultureInfo dateInfo = new System.Globalization.CultureInfo("en");

                string line;
                string[] split;
                StreamReader sr = new StreamReader(path, System.Text.Encoding.UTF8);

                while ((line = sr.ReadLine()) != null)
                {
                    row = table.NewRow();
                    split = line.Split(',');
                    DateTime drLST = Convert.ToDateTime(split[0].Trim('\0', '"') + " " + split[1].Trim('"'), dateInfo);
                    row["LST"] = drLST.ToString("yyyy-MM-dd HH:mm:00");
                    row["IR_880nm"] = split[7];
                    if (DateTime.Compare(m_LST, drLST) <= 0)
                        table.Rows.Add(row);
                }

                sr.Close();
                return table;
            }
            catch
            {
                return null;
            }
        }
    }
}

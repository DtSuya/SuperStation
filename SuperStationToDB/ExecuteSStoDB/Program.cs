using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Utility.SuperStation;
using ChinaAirUtility;
namespace ExecuteSStoDB
{
    class Program
    {
        static void Main(string[] args)
        {
            //args = new string[] { "{\"LST\":\"2017/03/17 00:59:00\",\"Type\":\"BC\"}", "{\"ConnectionString\":\"Data Source=211.144.122.59,3433;Initial Catalog=SEMCShare;Persist Security Info=True;User ID=sa;Password=diting\",\"TableName\":\"T_SuperStation1\",\"FilePath\":\"C:/Users/ZhangXiaoyi/Desktop/初始浓度.csv\",\"CreateTaskType\":\"MosaicLidar1\"}", "{}" }; //数据更换
            //args = new string[] { "{\"LST\":\"2017/01/03 00:59:00\",\"Type\":\"BC\"}", "{\"ConnectionString\":\"Data Source=10.1.14.62;Initial Catalog=SEMCShare;Persist Security Info=True;User ID=sa;Password=Diting2017\",\"TableName\":\"T_SuperStation1\",\"FilePath\":\"E:/SMMCDatabase/SourceData/BC/\",\"CreateTaskType\":\"\"}", "{}" };

            ResolvePar resolvePar = new ResolvePar(args);
            string ConnectionStrings = resolvePar.TryGetValue("ConnectionString");
            string type = resolvePar.TryGetValue("Type");
            DateTime LST = Convert.ToDateTime(resolvePar.TryGetValue("LST"));
            //string tablename = resolvePar.TryGetValue("TableName");
            string filepath = resolvePar.TryGetValue("FilePath");
            SuperStation super = new SuperStation(ConnectionStrings);
            super.Station = "环科院站";
            super.City = "四川";
            super.Type = type;
            super.InsertToDB(LST, filepath);
        }
    }
}

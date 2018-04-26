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
            //args = new string[] { "{\"LST\":\"2017/01/03 00:59:00\",\"Type\":\"BC\"}", "{\"ConnectionString\":\"Data Source=10.1.14.62;Initial Catalog=SEMCShare;Persist Security Info=True;User ID=sa;Password=Diting2017\",\"FilePath\":\"E:/SMMCDatabase/SourceData/BC/\"}", "{}" };
            //args = new string[] { "{\"LST\":\"2016/06/11 22:00:00\"}", "{\"Type\":\"ScatCo\",\"ConnectionString\":\"Data Source=10.1.14.62;Initial Catalog=SEMCShare;Persist Security Info=True;User ID=sa;Password=Diting2017\",\"FilePath\":\"E:/SMMCDatabase/SourceData/Zhuoduyi/\"}", "{}" };
            //args = new string[] { "{\"LST\":\"2018/03/29 02:00:00\"}", "{\"Type\":\"OCEC\",\"ConnectionString\":\"Data Source=10.1.14.62;Initial Catalog=SEMCShare;Persist Security Info=True;User ID=sa;Password=Diting2017\",\"FilePath\":\"E:/SMMCDatabase/SourceData/OCEC/\"}", "{}" };
            //args = new string[] { "{\"LST\":\"2018/03/13 00:59:00\"}", "{\"Type\":\"Spectra\",\"ConnectionString\":\"Data Source=10.1.14.62;Initial Catalog=SEMCShare;Persist Security Info=True;User ID=sa;Password=Diting2017\",\"FilePath\":\"E:/SMMCDatabase/SourceData/SPAMS/\"}", "{}" };
            //args = new string[] { "{\"LST\":\"2018/03/13 16:59:00\"}", "{\"Type\":\"Spectra\",\"ConnectionString\":\"Data Source=localhost;Initial Catalog=SEMCShare;Persist Security Info=True;User ID=sa;Password=diting\",\"FilePath\":\"E:/1Readearth/S四川环科院/超级站/tempdata/单颗粒质谱\"}", "{}" };
            //args = new string[] { "{\"LST\":\"2018/03/29 02:00:00\"}", "{\"Type\":\"OCEC\",\"ConnectionString\":\"Data Source=localhost;Initial Catalog=SEMCShare;Persist Security Info=True;User ID=sa;Password=diting\",\"FilePath\":\"E:/1Readearth/S四川环科院/超级站/tempdata/OCEC/\"}", "{}" };

            ResolvePar resolvePar = new ResolvePar(args);
            string ConnectionStrings = resolvePar.TryGetValue("ConnectionString");
            DateTime LST = Convert.ToDateTime(resolvePar.TryGetValue("LST"));
            string filepath = resolvePar.TryGetValue("FilePath");
            string type = resolvePar.TryGetValue("Type");
            //string tablename = resolvePar.TryGetValue("TableName");
            SuperStation super = new SuperStation(ConnectionStrings);
            super.Station = "环科院站";
            super.City = "四川";
            super.Type = type;
            if (super.Type == "Hmetal")
                super.InsertToDBHmetal(LST, filepath);
            else if (super.Type == "BC" || super.Type == "ScatCo")
                super.InsertToDBMin(LST, filepath);
            else if (super.Type == "OCEC" || super.Type == "Spectra")
                super.InsertToDBHour(LST, filepath);
            else
                super.InsertToDB(filepath);
        }
    }
}

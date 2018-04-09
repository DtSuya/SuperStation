using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Drawing;
using System.IO;

namespace Utility.SuperStation
{
    class LidarCacheNew
    {
        private double maxvalue;
        private int pixelsPerLine, lline;
        private int m_Timeindex;
        private string picpath;//生成切片的存放路径
        private string producname;
        private string type;
        private int m_PerPicMinutes;
        private int totalcount;

        public string Type
        {
            get { return type; }
            set { type = value; }
        }

        public string Producname
        {
            get { return producname; }
            set { producname = value; }
        }

        public double Maxvalue
        {
            get { return maxvalue; }
            set { maxvalue = value; }
        }
        public int Lline
        {
            get { return lline; }
            set { lline = value; }
        }
        public int TotalCount
        {
            get { return totalcount; }
            set { totalcount = value; }
        }
        public int PixelsPerLine
        {
            get { return pixelsPerLine; }
            set { pixelsPerLine = value; }
        }

        public int Timeindex
        {
            get { return m_Timeindex; }
            set { m_Timeindex = value; }
        }

        public LidarCacheNew(string picpath,int perPicMinutes)
        {
            this.picpath = picpath.EndsWith("\\") ? picpath : picpath + "\\";
            maxvalue = 1;//new 
            pixelsPerLine = 1;
            m_Timeindex = 0;
            type = "xiaoguang";
            m_PerPicMinutes = perPicMinutes;
        }


        public void DoCreateNew(string[] datastring)
        {
            DateTime dtime = DateTime.Parse(datastring[0]);
            string subpath = type + "\\" + dtime.ToString("yyyy") + "\\" + dtime.ToString("yyyyMMdd") + "\\";
            string picname = type + "_" + dtime.ToString("yyyyMMddHHmm00.PNG");
            //this.picpath = "Z:\\DSHLidarCache\\";
            Color[,] datacolor;
            if (type == "particle")
            {
                Color[] col = new Color[8];
                col[0] = Color.FromArgb(0, 128, 0);
                col[1] = Color.FromArgb(255, 0, 255);
                col[2] = Color.FromArgb(255, 255, 255);
                col[3] = Color.FromArgb(255, 0, 0);
                col[4] = Color.FromArgb(0, 0, 128);
                col[5] = Color.FromArgb(0, 255, 0);
                col[6] = Color.FromArgb(255, 255, 0);
                col[7] = Color.FromArgb(128, 128, 128);
                datacolor = GetQirj(datastring, col);
            }
            else
            {
                datacolor = ReadData(datastring);
            }
            //totalcount=414;
            CreateCache(datacolor, picpath + subpath + picname, TotalCount , Lline);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datastring">包含日期的数据字符串,以"\t"分隔</param>
        /// <returns></returns>
        private Color[,] ReadData(string[] dataArray)
        {
            Color[,] data = new Color[0, 0];
            if (dataArray.Length > 1)
            {
                data = new Color[dataArray.Length - 1, 1];
                //设置颜色
                Color[] cl = new Color[240];

                double steps = maxvalue / 240;//最大值现在为1

                if (type == "tuipian")
                    cl = GetColor_T();
                else
                    cl = GetColor();

                //53为400米附近数据所在的高度
                for (int i = 1; i < dataArray.Length; i++)//需要更改
                {
                    double v = 0;
                    try
                    {
                        double v1 = 0;
                        double v2 = Convert.ToDouble(dataArray[i].Trim());
                        try
                        {
                            v1 = Convert.ToDouble(dataArray[i + 1].Trim());
                        }
                        catch { v1 = v2; }
                        v = (v1 + v2) / 2;

                        if ((v1 - maxvalue) > (1 * maxvalue) && type == "tuipian")
                            v = 0;

                        if ((v2 - maxvalue) > (1 * maxvalue) && type == "tuipian")
                            v = 0;
                        if ((v - maxvalue) > 0 && type.Contains("O3Leida"))
                            v = maxvalue;

                        //if ((v2 - maxvalue) > 0 && type.Contains("O3Leida"))
                        //    v = maxvalue;
                        if (v1 < 0 || v2 < 0)
                            v = 0;
                    }
                    catch
                    {
                        v = -999;
                        break;
                    }
                    int colorstep = (int)(v / steps);
                    if (colorstep > cl.Length - 1)
                        colorstep = cl.Length - 1;
                    if (colorstep < 0)
                        colorstep = 0;

                    if (v >= 0)//>=0等于0的值也是需要的
                    {
                        data[i - 1, 0] = cl[colorstep];
                    }
                }
            }
            return data;
        }

        private static Color[] GetColor()
        {
            Color[] color = new Color[240];
            int[] cor = { 0, 0, 0 };
            double rung = 5.5;
            for (int i = 0; i < 240; i++)
            {
                if (i < 48)
                {
                    cor[2] = (int)(i * rung);
                    if (cor[2] > 255)
                        cor[2] = 255;
                }
                if (i >= 48 && i < 96)
                {
                    cor[1] = (int)((i - 47) * rung);
                    if (cor[1] > 255)
                        cor[1] = 255;
                    cor[2] = (int)(255 - (i - 47) * rung);
                    if (cor[2] < 0)
                        cor[2] = 0;
                }
                if (i >= 96 && i < 144)
                {
                    cor[0] = (int)((i - 95) * rung);
                    if (cor[0] > 255)
                        cor[0] = 255;
                }
                if (i >= 144 && i < 192)
                {
                    cor[1] = (int)(255 - (i - 143) * rung * 0.5);
                    if (cor[1] < 0)
                        cor[1] = 0;
                }
                if (i >= 192)
                {
                    cor[1] = (int)((255 - (i - 191) * rung) * 0.5);
                    if (cor[1] < 0)
                        cor[1] = 0;
                    //cor[2] = (int)((i - 191) * rung);
                    //if (cor[2] > 255)
                    //    cor[2] = 255;
                }
                color[i] = Color.FromArgb(cor[0], cor[1], cor[2]);
            }
            return color;
        }

        private static Color[] GetColor_T()
        {
            Color[] color = new Color[240];
            int[] cor = { 0, 0, 0 };
            for (int i = 0; i < 240; i++)
            {
                if (i < 10)
                {
                    cor[2] = (int)(i * 28.3);
                    if (cor[2] > 255)
                        cor[2] = 255;
                }
                if (i >= 10 && i < 50)
                {
                    cor[1] = (int)((i - 9) * 6.4);
                    if (cor[1] > 255)
                        cor[1] = 255;
                    cor[2] = (int)(255 - (i - 9) * 6.4);
                    if (cor[2] < 0)
                        cor[2] = 0;
                }
                if (i >= 50 && i < 108)
                {
                    cor[0] = (int)((i - 49) * 4.4);
                    if (cor[0] > 255)
                        cor[0] = 255;
                }
                if (i >= 108 && i < 181)
                {
                    cor[1] = (int)(255 - (i - 107) * 3.5);
                    if (cor[1] < 0)
                        cor[1] = 0;
                }
                if (i >= 181)
                {
                    cor[1] = (int)((i - 180) * 3.5);
                    if (cor[1] > 255)
                        cor[1] = 255;
                    cor[2] = (int)((i - 180) * 3.5);
                    if (cor[2] > 255)
                        cor[2] = 255;
                }
                color[i] = Color.FromArgb(cor[0], cor[1], cor[2]);
            }
            return color;
        }

        private Color[,] GetQirj(string[] datastring, Color[] color)
        {
            Color[,] data = new Color[0, 0];
            if (datastring.Length > 1)
            {
                data = new Color[datastring.Length - 1, 1];//53为400米附近数据所在的高度
                for (int i = 1; i < datastring.Length; i++)//需要更改
                {
                    int v = int.Parse(datastring[i]);
                    data[i - 1, 0] = color[v];
                }
            }
            return data;
        }

        /// <summary>
        /// 根据输入的数据创建切片
        /// </summary>
        /// <param name="data"></param>
        /// <param name="picName"></param>
        /// <returns></returns>
        private void CreateCache(Color[,] data, string picName, int lineCount, int lineL)
        {
            int timeCount = 1;
            Bitmap bm = new Bitmap(timeCount * PixelsPerLine, lineCount * lineL);
            for (int jj = 0; jj < lineCount; jj++)
            {
                for (int n = 0; n < lineL; n++)
                {
                    for (int m = 0; m < pixelsPerLine; m++)
                    {
                        bm.SetPixel(m, (lineCount - jj) * lineL - n - 1, data[jj, 0]);
                    }
                }
            }
            string fpath = picName.Substring(0, picName.LastIndexOf("\\"));
            if (!Directory.Exists(fpath))
                Directory.CreateDirectory(fpath);
            bm.Save(picName, System.Drawing.Imaging.ImageFormat.Png);
        }


        /// <summary>
        /// 生成一天内的空白图像
        /// </summary>
        /// <param name="xpixels">空白图像</param>
        /// <param name="ypixels"></param>
        /// <param name="picDirectory"></param>
        /// <param name="type"></param>
        /// <param name="stime"></param>
        /// <param name="minutesInterval"></param>
        public void CreateVoidCache(int xpixels, int ypixels, string picDirectory, string type, DateTime stime, int minutesInterval)
        {
            Bitmap bmp = new Bitmap(xpixels, ypixels);
            stime = DateTime.Parse(stime.ToString("yyyy-MM-dd 00:00:00"));
            picDirectory = (picDirectory.EndsWith("\\")) ? picDirectory : picDirectory + "\\";
            string subpath = type + "\\" + stime.ToString("yyyy") + "\\" + stime.ToString("yyyyMMdd") + "\\";
            if (!Directory.Exists(picDirectory + subpath))
                Directory.CreateDirectory(picDirectory + subpath);
            string picname = type + "_" + stime.ToString("yyyyMMddHHmm00") + ".PNG";
            bmp.Save(picDirectory + subpath + picname, System.Drawing.Imaging.ImageFormat.Png);

            for (int i = 1; i < 240; i++)
            {
                stime = stime.AddMinutes(m_PerPicMinutes);
                string despicname = type + "_" + stime.ToString("yyyyMMddHHmm00") + ".PNG";
                File.Copy(picDirectory + subpath + picname, picDirectory + subpath + despicname);
            }

        }


        private DateTime CorrectTime(DateTime inputTime)
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
                    for (int j = 5; j <= 60; j += 5)
                    {
                        if (i % j == 0)
                        {
                            up = i;
                            down = i - 5;
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

        private void RepairHeight(string picname)
        {
            Bitmap bmp = new Bitmap(picname);
            int height = bmp.Height / 2;
            int width = bmp.Width;
        }

    }
}

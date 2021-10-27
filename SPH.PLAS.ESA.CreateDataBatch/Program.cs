using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using SPH.PLAS.ESA;

namespace SPH.PLAS.ESA.CreateDataBatch
{
    class Program
    {
        private static IStandardLog logger;
        private static ESAProperties properties;
        private static readonly string name = "emg";
        static void Main(string[] args)
        {
            string dir = AppDomain.CurrentDomain.BaseDirectory;
            DateTime now = DateTime.Now;
            NewtonJsonWrapper json = new NewtonJsonWrapper();
            properties = json.Read<ESAProperties>(string.Format("{0}esa.json", dir));
            LogProperties logProperties = properties.LogConfig;
            logger = new StandardLog(logProperties.FilePath, logProperties.LogLevel, logProperties.IsConsole);

            logger.Information("상황분석 데이터 생성 시작");
            string datasourcePath = string.Format("{0}\\{1}\\{2}.udbx", properties.DatasourcePath, name, now.ToString("yyyyMMddHHmmss"));
            File.Copy(string.Format("{0}/udb/{1}.udbx", dir, name), datasourcePath);
            //긴급상황분석용 데이터 생성
            CreateESAData createESAData = new CreateESAData(properties, logger);
            createESAData.SetLabel += SetLog;
            createESAData.SetProgressPosition += SetDatasetProgressPosition;
            createESAData.SetWProgreePosition += SetWholePropressPosition;
            createESAData.SetMaxProgress += SetDatasetProgressProperties;
            createESAData.SetESADatasource(datasourcePath);
            createESAData.MakeNetworkData();
            Util.CleanUDB(now, properties.DatasourcePath, properties.UdbLifePeriod, name);
            CleanLog(now);
            logger.Information("상황분석 데이터 생성 완료");
        }
        private static void CleanLog(DateTime now)
        {
            int udbLifePeriod = properties.UdbLifePeriod;            
            string logPath = properties.LogConfig.FilePath;
            logPath = logPath.Substring(0, logPath.LastIndexOf("\\"));
            string[] logFiles = Directory.GetFiles(logPath);
            foreach(string logFile in logFiles)
            {
                if(File.GetCreationTime(logFile).AddDays(udbLifePeriod*3) <= now)
                {
                    File.Delete(logFile);
                }
            }
        }
        private static void SetLog(string msg)
        {
            logger.Information(msg);
        }
        private static void SetDatasetProgressPosition(int position)
        {

        }
        private static void SetDatasetProgressProperties(int maximum)
        {

        }
        private static void SetWholePropressPosition(int position) { }
    }
}

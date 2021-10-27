using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace SPH.PLAS.ESA.ExportData
{
    class Program
    {
        private static IStandardLog logger;
        private static ESAProperties properties;
        private static readonly string name = "icgis";
        static void Main(string[] args)
        {            
            string dir = AppDomain.CurrentDomain.BaseDirectory;
            DateTime now = DateTime.Now;
            NewtonJsonWrapper json = new NewtonJsonWrapper();
            properties = json.Read<ESAProperties>(string.Format("{0}esa.json", dir));
            LogProperties logProperties = properties.LogConfig;
            logger = new StandardLog(logProperties.FilePath, logProperties.LogLevel, logProperties.IsConsole);

            logger.Information("데이터 Export 시작");
            string datasourcePath = string.Format("{0}\\icgis\\{1}.udbx", properties.DatasourcePath, now.ToString("yyyyMMddHHmmss"));
            File.Copy(string.Format("{0}/udb/{1}.udbx", dir, name), datasourcePath);
            ExportGISData exportData = new ExportGISData(properties, logger);
            exportData.SetDatasource(datasourcePath);
            exportData.Export();
            Util.CleanUDB(now, properties.DatasourcePath, properties.UdbLifePeriod, name);
            logger.Information("데이터 Export 완료");
        }
    }
}

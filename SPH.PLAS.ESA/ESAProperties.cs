using System.Collections.Generic;

namespace SPH.PLAS.ESA
{
    public struct ESAProperties
    {
        public string WorkspacePath { get; set; }
        public string DatasourcePath { get; set; }
        public string SourceDatasourceName { get; set; }
        public string TargetDatasourceName { get; set; }
        public int UdbLifePeriod { get; set; }
        public Dictionary<string, DatasetProperties> Datasets { get; set; }
        public LogProperties LogConfig { get; set; }
        public string SourceNodeWhere { get; set; }
        public string LpSourceNodeWhere { get; set; }
        public string ValveOpenValue { get; set; }
        public string IsValveWhere { get; set; }
        public string LPValue { get; set; }
        public List<string> ExportDatasets { get; set; }
        public ESAProperties(string workspacePath, string datasourcePath, string sourceDatasourceName, string targetDatasourceName, int udbLifePeriod, LogProperties logConfig, Dictionary<string, DatasetProperties> datasets, string sourceNodeWhere, string lpSourceNodeWhere, string valueOpenValue, string isValveWhere, string lpValue, List<string> exportDatasets)
        {
            WorkspacePath = workspacePath;
            DatasourcePath = datasourcePath;
            SourceDatasourceName = sourceDatasourceName;
            TargetDatasourceName = targetDatasourceName;
            UdbLifePeriod = udbLifePeriod;
            LogConfig = logConfig;
            Datasets = datasets;
            SourceNodeWhere = sourceNodeWhere;
            LpSourceNodeWhere = lpSourceNodeWhere;
            ValveOpenValue = valueOpenValue;
            IsValveWhere = isValveWhere;
            LPValue = lpValue;
            ExportDatasets = exportDatasets;
        }
        public static ESAProperties Initialize()
        {
            Dictionary<string, DatasetProperties> datasets = new Dictionary<string, DatasetProperties>();
            Dictionary<string, string> pipeFields = new Dictionary<string, string>();
            pipeFields.Add("Code", "PIPE_MNG_NO");
            pipeFields.Add("KeyValue", "PIPE_CHRCTR_NO");
            pipeFields.Add("Label", "PROJECT_ID");
            pipeFields.Add("Name", "GAS_OPEN_DT");
            pipeFields.Add("ConstNo", "PROJECT_ID");
            pipeFields.Add("Diameter", "PIPDMTR_CD");
            pipeFields.Add("Length", "PIPE_LEN");
            pipeFields.Add("Material", "QMAT_CD");
            pipeFields.Add("PressureType", "PRSR_DVSN_CD");
            pipeFields.Add("ServiceStat", "ENABLED");
            pipeFields.Add("PipeType", "PIPE_KIND_CD");
            pipeFields.Add("PipeId", "FILE_DONG_CD||FILE_DVSN_CD||FILE_NO");
            DatasetProperties pipeDataset = new DatasetProperties("PIPELINE", "GAS_PIPE", pipeFields, "1=1");
            datasets.Add("PIPELINE", pipeDataset);

            Dictionary<string, string> regulatorFields = new Dictionary<string, string>();
            regulatorFields.Add("Code", "GOV_KIND_CD");
            regulatorFields.Add("KeyValue", "GOV_ID");
            regulatorFields.Add("Label", "GOV_MNG_NO");
            regulatorFields.Add("Name", "GOV_NM");
            regulatorFields.Add("ConstNo", "PROJECT_ID");
            regulatorFields.Add("ServiceStat", "ENABLED");
            regulatorFields.Add("NodeType", "4");
            DatasetProperties regulatorDataset = new DatasetProperties("REGULATOR", "GOV", regulatorFields, "GOV_KIND_CD in ('단독정압기','지구정압기','지역정압기','구역형압력조정기')");
            datasets.Add("REGULATOR", regulatorDataset);

            Dictionary<string, string> valveFields = new Dictionary<string, string>();
            valveFields.Add("Code", "VALVE_MNG_NO");
            valveFields.Add("KeyValue", "VALVE_ID");
            valveFields.Add("Label", "VALVE_MNG_NO");
            valveFields.Add("Name", "PROJECT_NAME");
            valveFields.Add("ConstNo", "PIPE_MNG_NO");
            valveFields.Add("OpenYN", "ISOPEN");
            valveFields.Add("ServiceStat", "ENABLED");
            valveFields.Add("NodeType", "3");
            DatasetProperties valveDataset = new DatasetProperties("VALVE", "VALVE", valveFields, "1=1");
            datasets.Add("VALVE", valveDataset);

            Dictionary<string, string> buildingFields = new Dictionary<string, string>();
            buildingFields.Add("Code", "REP_RESRC");
            buildingFields.Add("Name", "REP_RESOURCE_NAME_FST");
            buildingFields.Add("KeyValue", "REP_RESRC");
            buildingFields.Add("PipeId", "FILE_DONG_CD||FILE_DVSN_CD||FILE_NO");
            DatasetProperties buildingDataset = new DatasetProperties("BUILDING", "USE_GROUP", buildingFields, "1 = 1");
            datasets.Add("BUILDING", buildingDataset);

            ESAProperties config = new ESAProperties(@"C:\supermap\test.smwu",@"C:\supermap\udb","icgis", "emg",10,
                new LogProperties("info", @"C:\supermap\logs\log.log",true),
                datasets, "Code = '지구정압기'", "Code in ('지역정압기','구역형압력조정기')", "'Y'", "(NodeType=3 or NodeType=4)", "'저압'", new List<string>());
            return config;
        }
    }

    public struct DatasetProperties
    {
        public DatasetProperties(string type, string name, Dictionary<string, string>fieldsProperties, string where)
        {
            Type = type;
            Name = name;
            Fields = fieldsProperties;
            Where = where;
        }
        public string Type { get; set; }
        public string Name { get; set; }
        public Dictionary<string, string> Fields { get; set; }
        public string Where { get; set; }
    }

    public struct LogProperties
    {
        public LogProperties(string logLevel, string filePath, bool isConsole)
        {
            LogLevel = logLevel;
            FilePath = filePath;
            IsConsole = isConsole;
        }

        public string LogLevel { get; set; }
        public string FilePath { get; set; }
        public bool IsConsole { get; set; }
    }
}

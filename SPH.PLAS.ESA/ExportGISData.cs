using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SuperMap.Data;
using SuperMap.Analyst.NetworkAnalyst;
using SuperMap.Data.Topology;
using SuperMap.Analyst.SpatialAnalyst;
namespace SPH.PLAS.ESA
{
    public class ExportGISData
    {
        private Workspace m_workspace;
        private ESAProperties m_properties;
        private IStandardLog m_standardLog;

        public ExportGISData(ESAProperties esaProperties = default, IStandardLog standardLog = null)
        {
            SetProperties(esaProperties);
            SetLog(standardLog);
            WorkspaceConnectionInfo info = new WorkspaceConnectionInfo(m_properties.WorkspacePath);
            m_workspace = new Workspace();
            m_workspace.Open(info);
        }

        private void SetProperties(ESAProperties esaProperties)
        {
            if (esaProperties.Equals(default(ESAProperties))) {
                NewtonJsonWrapper newtonJson = new NewtonJsonWrapper();
                m_properties = newtonJson.Read<ESAProperties>("esa.json");
            }
            else
            {
                m_properties = esaProperties;
            }
        }
        private void SetLog(IStandardLog standardLog)
        {
            if (standardLog == null)
            {
                LogProperties logConfig = m_properties.LogConfig;
                m_standardLog = new StandardLog(logConfig.FilePath, logConfig.LogLevel, logConfig.IsConsole);
            }
            else
            {
                m_standardLog = standardLog;
            }
        }

        public void SetDatasource(string datasourcePath)
        {
            DatasourceConnectionInfo info = new DatasourceConnectionInfo(datasourcePath, m_properties.TargetDatasourceName, null);
            info.EngineType = EngineType.UDBX;
            if (m_workspace.Datasources.Contains(m_properties.TargetDatasourceName))
            {
                m_workspace.Datasources.Close(m_properties.TargetDatasourceName);
            }
            m_workspace.Datasources.Open(info);
        }
        public void Export()
        {
            Datasource targetDatasource = m_workspace.Datasources[m_properties.TargetDatasourceName];
            Datasource sourceDatasource = m_workspace.Datasources[m_properties.SourceDatasourceName];
            foreach(string datasetName in m_properties.ExportDatasets)
            {
                try
                {
                    DatasetVector sourceDataset = sourceDatasource.Datasets[datasetName] as DatasetVector;
                    Dictionary<string,string> fieldIndexs = sourceDataset.GetFieldIndexes();
                    SpatialIndexType indexType = sourceDataset.SpatialIndexType;
                    DatasetVector vector = targetDatasource.CopyDataset(sourceDataset, datasetName, EncodeType.None) as DatasetVector;
                    if(vector != null)
                    {
                        vector.Tolerance.NodeSnap = 0.0001;
                        vector.Tolerance.Dangle = 0.001;
                        vector.Tolerance.Extend = 0.001;
                        if(indexType != SpatialIndexType.None)
                        {
                            vector.Close();
                            vector.BuildSpatialIndex(SpatialIndexType.RTree);
                        }
                    }
                    else if(sourceDataset.Type.Equals(DatasetType.LinkTable))
                    {
                        DatasetVectorInfo info = new DatasetVectorInfo(datasetName, DatasetType.Tabular);
                        vector = targetDatasource.Datasets.Create(info);
                        FieldInfos sourceFieldInfos = sourceDataset.FieldInfos;
                        for(int i = 0; i < sourceFieldInfos.Count; i++)
                        {
                            vector.FieldInfos.Add(sourceFieldInfos[i].Clone());
                        }
                        using(Recordset tabularRs = vector.GetRecordset(true, CursorType.Dynamic))
                        {
                            Recordset.BatchEditor editor = tabularRs.Batch;
                            editor.Begin();
                            using(Recordset sourceRs = sourceDataset.GetRecordset(false, CursorType.Static))
                            {
                                while (!sourceRs.IsEOF)
                                {
                                    Dictionary<string, object> values = new Dictionary<string, object>();
                                    for(int i = 0; i < sourceFieldInfos.Count; i++)
                                    {
                                        values.Add(sourceFieldInfos[i].Name, sourceRs.GetObject(i));
                                    }
                                    tabularRs.AddNew(null, values);
                                    sourceRs.MoveNext();
                                }
                            }
                            editor.Update();
                        }
                    }
                    foreach (KeyValuePair<string, string> indexItem in fieldIndexs)
                    {
                        vector.BuildFieldIndex(indexItem.Value.Split(','), indexItem.Key);
                    }
                    m_standardLog.Information(string.Format("{0} Copy 완료", datasetName));
                }
                catch(Exception e)
                {
                    m_standardLog.Error(e, string.Format("{0} - Copy Dataset Error", datasetName));
                }
            }
            m_workspace.Close();
        }
    }
}

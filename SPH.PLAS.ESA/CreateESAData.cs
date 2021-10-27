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
    public class CreateESAData
    {
        private Workspace m_workspace;

        private ESAProperties m_properties;
        private IStandardLog m_standardLog;
        private readonly double m_tolerance = 0.005;
        private string topologyDatasetName = "topoDv";
        public delegate void SetDatasetProgressProperties(int maximum);
        public delegate void SetDatasetProgressPosition(int position);
        public delegate void SetDatasetLabel(string label);
        public delegate void SetWholePropressPosition(int position);
        public event SetDatasetProgressProperties SetMaxProgress;
        public event SetDatasetProgressPosition SetProgressPosition;
        public event SetDatasetLabel SetLabel;
        public event SetWholePropressPosition SetWProgreePosition;

        public CreateESAData(ESAProperties esaProperties = default, IStandardLog standardLog = null)
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

        public void SetESADatasource(string datasourcePath)
        {
            DatasourceConnectionInfo info = new DatasourceConnectionInfo(datasourcePath, m_properties.TargetDatasourceName, null);
            info.EngineType = EngineType.UDBX;
            if (m_workspace.Datasources.Contains(m_properties.TargetDatasourceName))
            {
                m_workspace.Datasources.Close(m_properties.TargetDatasourceName);
            }
            m_workspace.Datasources.Open(info);
        }
        public void MakeNetworkData()
        {
            int cnt = 1;
            Datasource targetDatasource = m_workspace.Datasources[m_properties.TargetDatasourceName];
            Datasource sourceDatasource = m_workspace.Datasources[m_properties.SourceDatasourceName];
            DatasetProperties buildingProperties = default;
            string[] delDatasetArr = new string[] { "mp_network", "lp_network","topoDv","intersectDv" };

            List<DatasetVector> lineDatasets = new List<DatasetVector>();
            List<DatasetVector> pointDatasets = new List<DatasetVector>();
            List<string> lineFieldNames = new List<string>();
            List<string> pointFieldNames = new List<string>();
            DatasetProperties pipeDatasetProperties = default;

            targetDatasource.Datasets.Delete("network");
            targetDatasource.Datasets.Delete("mp_network");
            foreach (KeyValuePair<string, DatasetProperties> datasetProperties in m_properties.Datasets)
            {
                switch (datasetProperties.Key)
                {
                    case "PIPELINE":
                        SetLabel("배관데이터 복사중...");
                        break;
                    case "REGULATOR":
                        SetLabel("정압기데이터 복사중...");
                        break;
                    case "VALVE":
                        SetLabel("밸브데이터 복사중...");
                        break;
                    case "BUILDING":
                        SetLabel("수용가데이터 복사중...");
                        buildingProperties = datasetProperties.Value;
                        break;
                }
                DatasetVector datasetVector = CreateDataset(targetDatasource, sourceDatasource, datasetProperties.Value, null);
                if (datasetProperties.Key.Equals("PIPELINE"))
                {
                    pipeDatasetProperties = datasetProperties.Value;
                    lineDatasets.Add(datasetVector);
                    lineFieldNames = datasetProperties.Value.Fields.Keys.ToList();
                }
                else if (datasetProperties.Key.Equals("REGULATOR") || datasetProperties.Key.Equals("VALVE"))
                {
                    pointDatasets.Add(datasetVector);
                    pointFieldNames.AddRange(datasetProperties.Value.Fields.Keys);
                }
                SetWProgreePosition(cnt++);
            }
            SetLabel("분기 데이터 생성중...");
            pointDatasets.Add(CreatePipeDivideJunction(lineDatasets, pointDatasets));
            
            string mpWhere = string.Format("{0} AND {1} != {2}", pipeDatasetProperties.Where, pipeDatasetProperties.Fields["PressureType"], m_properties.LPValue);
            DatasetVector mpPipeDatasetVector = targetDatasource.Datasets["mp_pipe"] as DatasetVector;

            SetLabel("네트웍데이터 생성중...");
            DatasetVector network = NetworkBuilder.BuildNetwork(
                lineDatasets.ToArray(), pointDatasets.ToArray(),
                lineFieldNames.ToArray(), pointFieldNames.ToArray(),
                targetDatasource, "network", NetworkSplitMode.LineSplitByPoint, m_tolerance);
            bool isDirection = NetworkBuilder.BuildFacilityNetworkDirections(
                GetFacilityAnalystSetting(network),
                GetSourceNodeIds(network.ChildDataset, m_properties.SourceNodeWhere),
                null, "length", "NodeType1");
            network.Close();
            network.BuildSpatialIndex(SpatialIndexType.RTree);
            network.BuildFieldIndex(new string[] { "SmFNode" }, "network_SmFNode_IDX");
            network.BuildFieldIndex(new string[] { "SmTNode" }, "network_SmTNode_IDX");
            SetWProgreePosition(cnt++);

            SetLabel("중압 네트웍데이터 생성중...");
            DatasetVector mpNetwork = NetworkBuilder.BuildNetwork(
                new DatasetVector[] { mpPipeDatasetVector }, pointDatasets.ToArray(),
                lineFieldNames.ToArray(), pointFieldNames.ToArray(),
                targetDatasource, "mp_network", NetworkSplitMode.LineSplitByPoint, m_tolerance);
            isDirection = NetworkBuilder.BuildFacilityNetworkDirections(
                GetFacilityAnalystSetting(mpNetwork),
                GetSourceNodeIds(mpNetwork.ChildDataset, m_properties.SourceNodeWhere),
                null, "length", "NodeType1");
            mpNetwork.Close();
            mpNetwork.BuildSpatialIndex(SpatialIndexType.RTree);
            SetWProgreePosition(cnt++);

            SetLabel("가스(중.고압) 흐름 분석중...");
            SetNetworkDirection(network, mpNetwork, mpWhere);
            SetWProgreePosition(cnt++);

            SetLabel("수용가 노드 데이터 생성중...");
            SetCustomerNode(network);
            SetLabel("수용가 노드-건물 맵핑 데이터 생성중...");
            HashSet<int> customerNodeIds = CreatePipeLinkCustomer(targetDatasource);

            SetLabel("밸브 On/Off 데이터 생성중...");
            DatasetVector idsDv = targetDatasource.Datasets["ids"] as DatasetVector;
            idsDv.Truncate();
            int[] rgltIds = CreateValveIds(idsDv, network.ChildDataset, m_properties.IsValveWhere, customerNodeIds);
            SetLabel("Loop 정압기 분석중...");
            SetLoopRegulatorAndValve(network, rgltIds);
            SetWProgreePosition(cnt++);
            //SetLabel("정압기 주변 가스 흐름 분석중...");
            //SetRegulatorDirection(network);            

            /*foreach (string delDataset in delDatasetArr)
            {
                targetDatasource.Datasets.Delete(delDataset);
            }*/
            SetLabel("분석결과 Dataset 정리");
            ClearESADataset();
            SetWProgreePosition(cnt++);

            DatasourceConnectionInfo connInfo = new DatasourceConnectionInfo(targetDatasource.ConnectionInfo.Server, m_properties.TargetDatasourceName, null);
            connInfo.EngineType = EngineType.UDBX;
            connInfo.IsReadOnly = true;
            m_workspace.Datasources.Close(m_properties.TargetDatasourceName);
            m_workspace.Datasources.Open(connInfo);
            m_workspace.Save();
            m_workspace.Close();
        }
        private string[] GetResultFields(DatasetProperties datasetProperties)
        {
            Dictionary<string, string> fields = datasetProperties.Fields;
            string[] result = new string[fields.Count];
            int idx = 0;
            foreach(KeyValuePair<string, string> field in fields)
            {
                result[idx++] = string.Format("{0} AS {1}",field.Value, field.Key);
            }
            return result;
        }
        private DatasetVector CreateDataset(Datasource targetDatasource, Datasource sourceDatasource, DatasetProperties datasetProperties, string datasetName)
        {
            int cnt = 0;
            string targetDatasetName = datasetProperties.Type;
            if (datasetName != null)
            {
                targetDatasetName = datasetName;
            }
            if (targetDatasetName.Equals("PIPELINE"))
            {
                return CreatePipeDataset(targetDatasource, sourceDatasource, datasetProperties);
            }
            DatasetVector sourceDataset = sourceDatasource.Datasets[datasetProperties.Name] as DatasetVector;
            DatasetVector targetDataset = targetDatasource.Datasets[targetDatasetName] as DatasetVector;
            string where = datasetProperties.Where;

            targetDataset.Truncate();

            using (Recordset targetRecordset = targetDataset.GetRecordset(true, CursorType.Dynamic))
            {
                Recordset.BatchEditor editor = targetRecordset.Batch;
                editor.Begin();
                QueryParameter param = new QueryParameter();
                param.CursorType = CursorType.Static;
                param.AttributeFilter = where;
                param.ResultFields = GetResultFields(datasetProperties);
                using (Recordset sourceRecordset = sourceDataset.Query(param))
                {
                    SetMaxProgress(sourceRecordset.RecordCount);
                    while (!sourceRecordset.IsEOF)
                    {
                        cnt++;
                        if (cnt % 101 == 0)
                        {
                            SetProgressPosition(cnt);
                        }
                        targetRecordset.AddNew(sourceRecordset.GetGeometry());
                        SetFieldValue(targetRecordset, sourceRecordset, datasetProperties.Fields);
                        sourceRecordset.MoveNext();
                    }
                }
                editor.Update();
            }
            targetDataset.Close();
            if (targetDatasetName.Equals("BUILDING"))
            {
                targetDataset.BuildFieldIndex(new string[] { "Code" }, "idx_building_code");
            }
            targetDataset.BuildSpatialIndex(SpatialIndexType.RTree);
            return targetDataset;
        }

        private void SetFieldValue(Recordset targetRecordset, Recordset sourceRecordset, IDictionary<string, string> fieldsInfo)
        {
            foreach (KeyValuePair<string, string> field in fieldsInfo)
            {
                if (field.Key.Equals("NodeType"))
                {
                    targetRecordset.SetObject(field.Key, field.Value);
                }
                else
                {
                    targetRecordset.SetObject(field.Key, sourceRecordset.GetObject(field.Key));
                }
            }
        }

        private DatasetVector CreatePipeDataset(Datasource targetDatasource, Datasource sourceDatasource, DatasetProperties datasetProperties)
        {
            int cnt = 0;
            DatasetVector sourceDataset = sourceDatasource.Datasets[datasetProperties.Name] as DatasetVector;
            DatasetVector targetDatasetAll = targetDatasource.Datasets[datasetProperties.Type] as DatasetVector;
            DatasetVector targetDatasetMP = targetDatasource.Datasets["mp_pipe"] as DatasetVector;
            targetDatasetAll.Truncate();
            targetDatasetMP.Truncate();
            using (Recordset targetRecordsetAll = targetDatasetAll.GetRecordset(true, CursorType.Dynamic))
            {
                using (Recordset targetRecordsetMP = targetDatasetMP.GetRecordset(true, CursorType.Dynamic))
                {
                    Recordset.BatchEditor editorAll = targetRecordsetAll.Batch;
                    Recordset.BatchEditor editorMP = targetRecordsetMP.Batch;
                    editorAll.Begin();
                    editorMP.Begin();
                    try
                    {
                        QueryParameter param = new QueryParameter();
                        param.CursorType = CursorType.Static;
                        param.AttributeFilter = datasetProperties.Where;
                        param.ResultFields = GetResultFields(datasetProperties);
                        using (Recordset sourceRecordset = sourceDataset.Query(param))
                        {
                            SetMaxProgress(sourceRecordset.RecordCount);
                            while (!sourceRecordset.IsEOF)
                            {
                                cnt++;
                                if (cnt % 51 == 0)
                                {
                                    SetProgressPosition(cnt);
                                }
                                targetRecordsetAll.AddNew(sourceRecordset.GetGeometry());
                                string pressureType = sourceRecordset.GetString("PressureType");
                                if (pressureType == null || "".Equals(pressureType))
                                {

                                }
                                else if (!pressureType.Equals(m_properties.LPValue.Replace("'", "")))
                                {
                                    //mp
                                    targetRecordsetMP.AddNew(sourceRecordset.GetGeometry());
                                    SetFieldValue(targetRecordsetMP, sourceRecordset, datasetProperties.Fields);
                                }
                                SetFieldValue(targetRecordsetAll, sourceRecordset, datasetProperties.Fields);
                                sourceRecordset.MoveNext();
                            }
                        }
                    } catch (Exception e)
                    {
                        m_standardLog.Error(e, "배관데이터 복사 중 오류");
                    }
                    editorAll.Update();
                    editorMP.Update();
                }
            }
            targetDatasetAll.Close();
            targetDatasetAll.BuildSpatialIndex(SpatialIndexType.RTree);
            return targetDatasetAll;
        }
        private DatasetVector CreateTopoDatasetVector(Datasource ds)
        {
            DatasetVector datasetVector = default;
            if(ds.Datasets.Contains(topologyDatasetName))
            {
                datasetVector =  ds.Datasets[topologyDatasetName] as DatasetVector;
            }
            else
            {
                datasetVector = ds.Datasets.Create(new DatasetVectorInfo(topologyDatasetName, DatasetType.Point));
                datasetVector.FieldInfos.Add(new FieldInfo("NodeType", FieldType.Int16));
            }
            datasetVector.PrjCoordSys = ds.PrjCoordSys;
            return datasetVector;
        }
        private DatasetVector CreatePipeDivideJunction(List<DatasetVector> pipeDatasets, List<DatasetVector> pointDatasets)
        {
            Datasource ds = pipeDatasets[0].Datasource;
            DatasetVector topoDv = CreateTopoDatasetVector(ds);
            using (Recordset topoRs = topoDv.GetRecordset(true, CursorType.Dynamic))
            {
                Recordset.BatchEditor editor = topoRs.Batch;
                editor.Begin();
                foreach (DatasetVector pipeDataset in pipeDatasets)
                {
                    string intersectDatasetName = "intersectDv";
                    ds.Datasets.Delete(intersectDatasetName);

                    TopologyPreprocessOptions options = new TopologyPreprocessOptions();
                    options.AreVertexesSnapped = true;
                    options.AreVertexAdjusted = false;
                    int pointDatasetCnt = pointDatasets.Count;
                    DatasetVector[] topoVectors = new DatasetVector[pointDatasetCnt + 1];
                    int[] precisionOrders = new int[pointDatasetCnt + 1];
                    for (int i = 0; i < pointDatasets.Count; i++)
                    {
                        topoVectors[i] = pointDatasets[i];
                        precisionOrders[i] = 0;
                    }
                    topoVectors[pointDatasetCnt] = pipeDataset;
                    precisionOrders[pointDatasetCnt] = 1;
                    TopologyValidator.Preprocess(topoVectors, precisionOrders, options, m_tolerance);
                    DatasetVector intersectDataset = TopologyValidator.Validate(pipeDataset, null, TopologyRule.LineNoIntersectOrInteriorTouch, m_tolerance, null, ds, intersectDatasetName);
                    using (Recordset rs = intersectDataset.GetRecordset(false, CursorType.Static))
                    {
                        while (!rs.IsEOF)
                        {
                            int errorSmID1 = rs.GetInt32("ErrorObjectID_1");
                            int errorSmID2 = rs.GetInt32("ErrorObjectID_2");
                            int errorIdx1 = rs.GetInt32("ErrorIndexFrom_1");
                            int errorIdx2 = rs.GetInt32("ErrorIndexFrom_2");
                            using(Recordset pipeRs = pipeDataset.Query(new int[] { errorSmID1, errorSmID2}, CursorType.Static))
                            {
                                bool isDivide = false;
                                while (!pipeRs.IsEOF)
                                {
                                    int idx = errorIdx2;
                                    if (pipeRs.GetID().Equals(errorSmID1))
                                    {
                                        idx = errorIdx1;
                                    }
                                    GeoLine line = pipeRs.GetGeometry() as GeoLine;
                                    int lastIdx = -1;
                                    for(int i = 0; i < line.PartCount; i++)
                                    {
                                        Point2Ds point2Ds = line[i];
                                        lastIdx += point2Ds.Count;
                                    }
                                    if (idx == 0 || idx == lastIdx)
                                    {
                                        isDivide = !isDivide;
                                    }
                                    pipeRs.MoveNext();
                                }
                                if (isDivide)
                                {
                                    topoRs.AddNew(rs.GetGeometry());
                                    topoRs.SetInt16("NodeType", 0);
                                }
                            }
                            rs.MoveNext();
                        }
                    }
                    SetLabel("밸브, 정압기 분기 생성..");
                    foreach (DatasetVector pointDataset in pointDatasets)
                    {
                        AddFacilityDivideJunction(topoRs, pipeDataset, pointDataset);
                    }
                }
                editor.Update();
            }
            return topoDv;
        }
        private void SetCustomerNode(DatasetVector network)
        {
            HashSet<int> dangleid = new HashSet<int>();
            HashSet<int> connectedid = new HashSet<int>();
            using (Recordset rs = network.GetRecordset(false, CursorType.Static))
            {
                while (!rs.IsEOF)
                {
                    int fNode = rs.GetInt32("SmFNode");
                    int tNode = rs.GetInt32("SmTNode");
                    dangleid.Add(fNode);
                    dangleid.Add(tNode);
                    if (connectedid.Contains(fNode))
                    {
                        dangleid.Remove(fNode);
                    }
                    else
                    {
                        connectedid.Add(fNode);
                    }
                    if (connectedid.Contains(tNode))
                    {
                        dangleid.Remove(tNode);
                    }
                    else
                    {
                        connectedid.Add(tNode);
                    }
                    rs.MoveNext();
                }
            }
            connectedid.Clear();
            DatasetVector node = network.ChildDataset;
            using (Recordset rs = node.GetRecordset(false, CursorType.Dynamic))
            {
                Recordset.BatchEditor editor = rs.Batch;
                editor.Begin();
                foreach (int nodeid in dangleid)
                {
                    rs.SeekID(nodeid);
                    rs.SetInt16("NodeType1", 2);
                }
                editor.Update();
            }
            dangleid.Clear();
        }
        private HashSet<int> CreatePipeLinkCustomer(Datasource ds)
        {
            HashSet<int> result = new HashSet<int>();
            DatasetVector network = ds.Datasets["network"] as DatasetVector;
            DatasetVector networkNode = network.ChildDataset;
            DatasetVector building = ds.Datasets["BUILDING"] as DatasetVector;
            ds.Datasets.Delete("CustomerNode");
            DatasetVectorInfo info = new DatasetVectorInfo("CustomerNode", DatasetType.Point);
            DatasetVector customerNode = ds.Datasets.Create(info);
            customerNode.FieldInfos.Add(new FieldInfo("cnt", FieldType.Int32));
            customerNode.FieldInfos.Add(new FieldInfo("NodeID", FieldType.Int32));
            FieldInfo fieldInfo = new FieldInfo("Code", FieldType.WText);
            fieldInfo.MaxLength = 100;
            customerNode.FieldInfos.Add(fieldInfo);
            customerNode.PrjCoordSys = ds.PrjCoordSys;

            Dictionary<string, List<int>> chkCode = new Dictionary<string, List<int>>();
            Dictionary<int, int> updateId = new Dictionary<int, int>();
            using (Recordset customerRs = customerNode.GetRecordset(false, CursorType.Dynamic))
            {
                Recordset.BatchEditor editor = customerRs.Batch;
                editor.Begin();
                using (Recordset nodeRs = networkNode.Query("NodeType1 = 2", CursorType.Static))
                {
                    while (!nodeRs.IsEOF)
                    {
                        using (Recordset buildingRs = building.Query(nodeRs.GetGeometry(), 0.005, CursorType.Static))
                        {
                            while (!buildingRs.IsEOF)
                            {
                                customerRs.AddNew(nodeRs.GetGeometry());
                                int smid = customerRs.GetID();
                                updateId.Add(smid, 1);
                                int nodeId = nodeRs.GetInt32("SmNodeID");
                                string code = buildingRs.GetString("Code");
                                if (chkCode.ContainsKey(code))
                                {
                                    List<int> ids = chkCode[code];
                                    ids.Add(smid);
                                    int cnt = ids.Count;
                                    foreach (int id in ids)
                                    {
                                        updateId[id] = cnt;
                                    }
                                    chkCode[code] = ids;
                                }
                                else
                                {
                                    List<int> ids = new List<int>();
                                    ids.Add(smid);
                                    chkCode.Add(code, ids);
                                }

                                customerRs.SetString("Code", code);

                                result.Add(nodeId);
                                customerRs.SetInt32("NodeID", nodeId);
                                buildingRs.MoveNext();
                            }
                        }
                        nodeRs.MoveNext();
                    }
                    foreach (KeyValuePair<int, int> item in updateId)
                    {
                        customerRs.SeekID(item.Key);
                        customerRs.SetInt32("cnt", item.Value);
                    }
                }
                editor.Update();
            }
            customerNode.BuildFieldIndex(new string[] { "NodeID" }, "idx_customernode_nodeid");
            customerNode.BuildFieldIndex(new string[] { "Code" }, "idx_customernode_code");

            return result;
        }
        private void AddFacilityDivideJunction(Recordset divideRs, DatasetVector pipeDataset, DatasetVector pointDataset)
        {
            using (Recordset pointRs = pointDataset.GetRecordset(false, CursorType.Static))
            {
                while (!pointRs.IsEOF)
                {
                    GeoPoint point = pointRs.GetGeometry() as GeoPoint;
                    GeoLine pipeLine = new GeoLine();
                    QueryParameter param = new QueryParameter();
                    param.SpatialQueryMode = SpatialQueryMode.Intersect;
                    param.SpatialQueryObject = point;
                    param.CursorType = CursorType.Static;
                    using (Recordset rs = pipeDataset.Query(param))
                    {
                        while (!rs.IsEOF)
                        {
                            GeoLine line = rs.GetGeometry() as GeoLine;
                            for (int i = 0; i < line.PartCount; i++)
                            {
                                pipeLine = Geometrist.Union(pipeLine, new GeoLine(line[i])) as GeoLine;
                            }
                            rs.MoveNext();
                        }
                    }
                    if (pipeLine.PartCount > 0)
                    {
                        Point2Ds points = pipeLine[0];
                        int pointCnt = points.Count;
                        for (int i = 0; i < pointCnt; i++)
                        {
                            if (Geometrist.IsIdentical(point, new GeoPoint(points[i]),m_tolerance/2))
                            {
                                Point2D secPoint = points[i];
                                if (i != 0)
                                {
                                    Point2D firPoint = points[i - 1];
                                    divideRs.AddNew(GetDividing(secPoint, firPoint, m_tolerance*2, pipeDataset));
                                }
                                if (i != pointCnt - 1)
                                {
                                    Point2D thrPoint = points[i + 1];
                                    divideRs.AddNew(GetDividing(secPoint, thrPoint, m_tolerance * 2, pipeDataset));
                                }
                                break;
                            }
                        }
                    }
                    pointRs.MoveNext();
                }
            }
        }
        private GeoPoint GetDividing(Point2D fromPoint, Point2D toPoint, double length, DatasetVector pipeDataset)
        {
            GeoPoint p = new GeoPoint(toPoint);
            double L = Math.Sqrt(Math.Pow(fromPoint.X - toPoint.X, 2) + Math.Pow(fromPoint.Y - toPoint.Y, 2));
            if (L > length)
            {
                p = new GeoPoint(fromPoint.X + length * (toPoint.X - fromPoint.X) / L, fromPoint.Y + length * (toPoint.Y - fromPoint.Y) / L);
            }
            return p;
        }
        private FacilityAnalystSetting GetFacilityAnalystSetting(DatasetVector network)
        {
            FacilityAnalystSetting setting = new FacilityAnalystSetting();
            setting.NetworkDataset = network;
            setting.NodeIDField = "SmNodeID";
            setting.EdgeIDField = "SmEdgeID";
            setting.FNodeIDField = "SmFNode";
            setting.TNodeIDField = "SmTNode";
            setting.DirectionField = "Direction";
            setting.Tolerance = m_tolerance;

            WeightFieldInfo fieldInfo = new WeightFieldInfo();
            fieldInfo.Name = "length";
            fieldInfo.FTWeightField = "SmLength";
            fieldInfo.TFWeightField = "SmLength";
            WeightFieldInfos fieldInfos = new WeightFieldInfos();
            fieldInfos.Add(fieldInfo);
            setting.WeightFieldInfos = fieldInfos;

            return setting;
        }

        private int[] GetSourceNodeIds(DatasetVector datasetVector, string sourceNodeWhere)
        {
            List<int> nodeIds = new List<int>();
            using(Recordset recordset = datasetVector.Query(sourceNodeWhere, CursorType.Static))
            {
                while (!recordset.IsEOF)
                {
                    nodeIds.Add(recordset.GetID());
                    recordset.MoveNext();
                }
            }
            return nodeIds.ToArray();
        }

        private void SetNetworkDirection(DatasetVector targetDataset, DatasetVector sourceDataset, string whereClause)
        {
            whereClause = whereClause.Replace(m_properties.Datasets["PIPELINE"].Fields["PressureType"], "PressureType");
            whereClause += " AND Direction in (2, 3)";
            using (Recordset targetRs = targetDataset.Query(whereClause, CursorType.Dynamic))
            {
                Recordset.BatchEditor editor = targetRs.Batch;
                editor.Begin();
                while (!targetRs.IsEOF)
                {
                    QueryParameter param = new QueryParameter();
                    param.CursorType = CursorType.Static;
                    param.SpatialQueryMode = SpatialQueryMode.Contain;
                    param.SpatialQueryObject = targetRs.GetGeometry();
                    param.HasGeometry = false;
                    using (Recordset sourceRs = sourceDataset.Query(param))
                    {
                        if (sourceRs.RecordCount == 1)
                        {
                            targetRs.SetInt32("Direction", sourceRs.GetInt32("Direction"));
                        }
                    }
                    targetRs.MoveNext();
                }
                editor.Update();
            }
        }

        private void SetRegulatorDirection(DatasetVector network)
        {
            DatasetVector node = network.ChildDataset;
            QueryParameter param = new QueryParameter();
            param.ResultFields = new string[] { "SmEdgeID", "SmFNode", "SmTNode", "PressureType", "Direction" };
            param.HasGeometry = false;
            param.CursorType = CursorType.Dynamic;
            HashSet<int> valveDic = new HashSet<int>();
            using (Recordset nodeRs = node.Query("NodeType = 3", CursorType.Static))
            {
                while (!nodeRs.IsEOF)
                {
                    valveDic.Add(nodeRs.GetInt32("SmNodeID"));
                    nodeRs.MoveNext();
                }
            }
            using (Recordset nodeRs = node.Query(m_properties.LpSourceNodeWhere, CursorType.Static))
            {
                while (!nodeRs.IsEOF)
                {
                    int nodeid = nodeRs.GetInt32("SmNodeID");
                    param.AttributeFilter = string.Format("PressureType = '저압' AND Direction = 2 AND (SmFNode = {0} OR SmTNode = {0})", nodeid);
                    ForwardToLoop(network, param, nodeid, valveDic);
                    nodeRs.MoveNext();
                }
            }
        }

        private void ForwardToLoop(DatasetVector network, QueryParameter param, int nodeid, HashSet<int> valveDic)
        {
            //밸브까지만..
            if (!valveDic.Contains(nodeid))
            {
                using (Recordset networkRs = network.Query(param))
                {
                    //분기 edge가 없을때
                    if (networkRs.RecordCount == 1)
                    {
                        int smFNode = networkRs.GetInt32("SmFNode");
                        int smTNode = networkRs.GetInt32("SmTNode");
                        int direction = 0;
                        if (nodeid == smTNode)
                        {
                            direction = 1;
                            nodeid = smFNode;
                        }
                        else
                        {
                            nodeid = smTNode;
                        }
                        networkRs.Edit();
                        networkRs.SetInt32("Direction", direction);
                        networkRs.Update();
                        param.AttributeFilter = string.Format("SmEdgeID != {0} AND Direction = 2 AND (SmFNode = {1} OR SmTNode = {1})", networkRs.GetInt32("SmEdgeID"), nodeid);
                        ForwardToLoop(network, param, nodeid, valveDic);
                    }
                }
            }
        }

        private int[] CreateValveIds(DatasetVector targetDataset, DatasetVector sourceDataset, string whereClause, HashSet<int> customerNodeIds)
        {
            targetDataset.Truncate();
            int[] result;
            using (Recordset targetRs = targetDataset.GetRecordset(true, CursorType.Dynamic))
            {
                QueryParameter param = new QueryParameter();
                param.CursorType = CursorType.Static;
                param.AttributeFilter = whereClause;
                param.OrderBy = new string[] { "SMID" };
                using(Recordset sourceRs = sourceDataset.Query(param))
                {
                    HashSet<int> ids = new HashSet<int>();
                    List<int> barrierIds = new List<int>();
                    List<int> rgltIds = new List<int>();
                    while (!sourceRs.IsEOF)
                    {
                        int id = sourceRs.GetID();
                        ids.Add(id);
                        object obj = sourceRs.GetObject("OpenYN");
                        if ("4".Equals(sourceRs.GetString("NodeType"))){
                            rgltIds.Add(id);
                        }
                        if (obj != null && !obj.Equals(m_properties.ValveOpenValue.Replace("'","")))
                        {
                            barrierIds.Add(id);
                        }
                        sourceRs.MoveNext();
                    }
                    result = rgltIds.ToArray();
                    targetRs.AddNew(null);
                    targetRs.SetLongBinary("ids", Util.ConvertToByteArray(ids));
                    targetRs.SetLongBinary("rgltIds", Util.ConvertToByteArray(result));
                    targetRs.SetLongBinary("barrierIds", Util.ConvertToByteArray(barrierIds.ToArray()));
                    targetRs.SetLongBinary("customerNodes", Util.ConvertToByteArray(customerNodeIds));
                    targetRs.Update();
                }
            }
            return result;
        }
        private void SetLoopRegulatorAndValve(DatasetVector network, int[] rgltIds)
        {
            FieldInfo fieldInfo = network.ChildDataset.FieldInfos["RGLT_GROUP"];
            if(fieldInfo == null)
            {
                FieldInfo field = new FieldInfo("RGLT_GROUP", FieldType.Int16);
                network.ChildDataset.FieldInfos.Add(field);
                field = new FieldInfo("RGLT_CNT", FieldType.Int16);
                network.ChildDataset.FieldInfos.Add(field);
            }
            FacilityAnalyst analyst = new FacilityAnalyst();
            analyst.AnalystSetting = GetFacilityAnalystSetting(network);
            analyst.Load();
            SetLoopRegulator(analyst, network, rgltIds);
            analyst.Dispose();
        }
        private void SetLoopRegulator(FacilityAnalyst analyst, DatasetVector network, int[] rgltIds)
        {
            List<HashSet<int>> loopRegulatorIds = new List<HashSet<int>>();
            foreach (int rgltId in rgltIds)
            {
                using (Recordset rs = network.ChildDataset.Query(string.Format("{0} AND SmNodeID = {1}", m_properties.SourceNodeWhere, rgltId), CursorType.Static))
                {
                    if (!rs.IsEmpty)
                    {
                        continue;
                    }
                }
                HashSet<int> currentSet = new HashSet<int>();

                QueryParameter param = new QueryParameter();
                //param.AttributeFilter = string.Format("PressureType = '저압' AND (SmFNode = {0} OR SmTNode = {0})", rgltId);
                param.AttributeFilter = string.Format("(SmFNode = {0} OR SmTNode = {0})", rgltId);
                param.HasGeometry = false;
                param.CursorType = CursorType.Static;
                using (Recordset networkRs = network.Query(param))
                {
                    if (!networkRs.IsEmpty)
                    {
                        FacilityAnalystResult result = analyst.FindCriticalFacilitiesDownFromNode(rgltIds, rgltId, true);
                        if (result != null)
                        {
                            currentSet.UnionWith(result.Nodes);
                            int idx = -1;
                            for (int i = 0; i < loopRegulatorIds.Count; i++)
                            {
                                HashSet<int> regulatorIds = loopRegulatorIds[i];
                                if (regulatorIds.Contains(rgltId))
                                {
                                    idx = i;
                                    currentSet = regulatorIds;
                                    break;
                                }
                                foreach (int nodeId in result.Nodes)
                                {
                                    if (regulatorIds.Contains(nodeId))
                                    {
                                        idx = i;
                                        currentSet = regulatorIds;
                                        break;
                                    }
                                }
                            }
                            currentSet.Add(rgltId);
                            if (idx >= 0)
                            {
                                loopRegulatorIds[idx].UnionWith(result.Nodes);
                            }
                            else
                            {
                                loopRegulatorIds.Add(currentSet);
                            }
                        }
                    }
                }
            }
            for (int i = 0; i < loopRegulatorIds.Count; i++)
            {
                string filter = string.Format("SmNodeId in ({0})", string.Join(",", loopRegulatorIds[i]));
                network.ChildDataset.UpdateField("RGLT_GROUP", i + 1, filter);
                network.ChildDataset.UpdateField("RGLT_CNT", loopRegulatorIds[i].Count, filter);
            }
            loopRegulatorIds.Clear();
        }
        private List<int> SetLoopValve(FacilityAnalyst analyst, DatasetVector network)
        {
            List<int> loopValveIds = new List<int>();
            int[] loopEdges = analyst.CheckLoops();
            return loopValveIds;
        }
        private void ClearESADataset()
        {
            Datasource datasource = m_workspace.Datasources[m_properties.SourceDatasourceName];
            string[] datasetNames = new string[] { "ESA_VIRTUALVALVE", "ESA_RESULT", "ESA_VALVE", "ESA_PIPELINE", "ESA_BUILDING", "ESA_THROWVALVE" };
            foreach(string datasetName in datasetNames)
            {
                DatasetVector dv = datasource.Datasets[datasetName] as DatasetVector;
                bool isTruncate = true;
                try
                {
                    isTruncate = dv.Truncate();
                }catch(Exception e)
                {
                    isTruncate = false;
                    m_standardLog.Error(e, string.Format("{0}Dataset TRUNCATE ERROR", datasetName));
                }
                if (!isTruncate)
                {
                    using(Recordset rs = dv.GetRecordset(false, CursorType.Dynamic))
                    {
                        rs.DeleteAll();
                    }
                }
            }
        }
    }
}

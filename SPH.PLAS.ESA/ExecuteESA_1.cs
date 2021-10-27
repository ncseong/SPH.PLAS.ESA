using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SuperMap.Data;
using SuperMap.Analyst.NetworkAnalyst;
using System.Data;

namespace SPH.PLAS.ESA
{
    public class ExecuteESA_1
    {
        private ExecuteESA_1 Instace = new ExecuteESA_1();
        private Workspace m_workspace;
        private ESAProperties m_properties;
        private IStandardLog m_standardLog;
        private readonly double m_tolerance = 0.005;
        public System.Data.DataSet m_gridDataset = new DataSet();
        
        public Rectangle2D m_boundary;
        private HashSet<int> m_pipeIds = new HashSet<int>();
        private IDictionary<string, PipeProperties> pipeLabel = new Dictionary<string, PipeProperties>();
        private IDictionary<string, string> buildingCode = new Dictionary<string, string>();
        private IDictionary<string, double> m_pipeLength = new Dictionary<string, double>();
        private IDictionary<string, double> m_users = new Dictionary<string, double>();
        private ExecuteESA_1(Workspace workspace = null, ESAProperties esaProperties = default, IStandardLog standardLog = null)
        {
            SetProperties(esaProperties);
            SetLog(standardLog);
            if (workspace == null)
            {
                WorkspaceConnectionInfo info = new WorkspaceConnectionInfo(m_properties.WorkspacePath);
                m_workspace = new Workspace();
                m_workspace.Open(info);
            }
            else
            {
                m_workspace = workspace;
            }
        }
        private void SetProperties(ESAProperties esaProperties)
        {
            if (esaProperties.Equals(default(ESAProperties)))
            {
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
        private FacilityAnalystSetting GetFacilityAnalystSetting(DatasetVector networkDv, int[] barrierIds)
        {
            FacilityAnalystSetting setting = new FacilityAnalystSetting();
            setting.NetworkDataset = networkDv;
            setting.NodeIDField = "SmNodeId";
            setting.EdgeIDField = "SmId";
            setting.FNodeIDField = "SmFNode";
            setting.TNodeIDField = "SmTNode";
            setting.DirectionField = "Direction";
            setting.Tolerance = m_tolerance;
            setting.BarrierNodes = barrierIds;
            WeightFieldInfo fieldInfo = new WeightFieldInfo();
            fieldInfo.Name = "length";
            fieldInfo.FTWeightField = "SmLength";
            fieldInfo.TFWeightField = "SmLength";
            WeightFieldInfos fieldInfos = new WeightFieldInfos();
            fieldInfos.Add(fieldInfo);
            setting.WeightFieldInfos = fieldInfos;
            return setting;
        }
        /*
        public bool ExecuteSituationAnalyst(string rgltNo)
        {
            Datasource sourceDatasource = m_workspace.Datasources[m_properties.TargetDatasourceName];
            Datasource targetDatasource = m_workspace.Datasources[m_properties.SourceDatasourceName];

            bool isPipe = false;
            if (facility.Contains("배관"))
            {
                isPipe = true;
            }
            
            DatasetVector networkDv = sourceDatasource.Datasets["network"] as DatasetVector;
            int[] valveIds = null;
            int[] barrierIds = null;
            int[] rgltIds = null;
            IDictionary<int, IList<int>> closeEdge = null;
            m_standardLog.Debug("Facility Analyst Setting");
            GetNodeIds(sourceDatasource, regnCd, out valveIds, out barrierIds, out closeEdge, out rgltIds);
            FacilityAnalystSetting setting = GetFacilityAnalystSetting(networkDv, barrierIds);
            FacilityAnalyst analyst = new FacilityAnalyst();
            analyst.AnalystSetting = setting;
            analyst.Load();
            m_standardLog.Debug("Facility Analyst Load");

            BurstAnalyseResult burstResult = null;
            FacilityAnalystResult facilityResult1 = null;
            HashSet<int> firstValves = new HashSet<int>();
            HashSet<int> secondValves = new HashSet<int>();
            HashSet<int> checkValves = new HashSet<int>();
            HashSet<int> firstPipes = new HashSet<int>();
            HashSet<int> secondPipes = new HashSet<int>();

            m_standardLog.Debug("상황분석 시작");
            if (isPipe)
            {
                burstResult = analyst.BurstAnalyseFromEdge(valveIds, id, true);
                facilityResult1 = analyst.FindCriticalFacilitiesUpFromEdge(valveIds, id, true);
            }
            else
            {
                burstResult = analyst.BurstAnalyseFromNode(valveIds, id, true);
                facilityResult1 = analyst.FindCriticalFacilitiesUpFromNode(valveIds, id, true);
            }
            if(facilityResult1 == null || burstResult == null)
            {
                return false;
            }
            m_standardLog.Debug("1차 밸브, 1차 배관 분석 완료");
            //1차 차단 밸브
            firstValves.UnionWith(facilityResult1.Nodes);
            secondValves.UnionWith(burstResult.CriticalNodes);
            secondValves.UnionWith(burstResult.NormalNodes);
            secondValves.ExceptWith(firstValves);
            m_standardLog.Debug("2차 밸브 분석 완료");
            //1차 차단 배관
            firstPipes.UnionWith(burstResult.Edges);
            try
            {
                IList<int> firstCloseEdge = closeEdge.Keys.Intersect(firstPipes).ToList();
                foreach (int edgeId in firstCloseEdge)
                {
                    firstPipes.UnionWith(closeEdge[edgeId]);
                }
            } catch (Exception ex) { }

            //2차 배관
            if(burstResult.NormalNodes.Length > 0)
            {
                foreach(int nodeId in burstResult.NormalNodes){
                    FacilityAnalystResult result = analyst.TraceDownFromNode(nodeId, "length", false);
                    if (result != null && result.Edges != null)
                    {
                        secondPipes.UnionWith(result.Edges);
                    }
                }
            }
            //HashSet<int> barrierNodes = new HashSet<int>();
            checkValves.UnionWith(secondValves);
            checkValves.ExceptWith(burstResult.NormalNodes);
            HashSet<int> barrierNodes = GetClosingRegulator(analyst, rgltIds, checkValves);
            analyst.Dispose();
            setting.BarrierNodes = null;
            analyst = new FacilityAnalyst();
            barrierNodes.UnionWith(barrierIds);
            barrierNodes.UnionWith(firstValves);
            setting.BarrierNodes = barrierNodes.ToArray();
            analyst.AnalystSetting = setting;
            m_standardLog.Debug("Facility Analyst Setting");
            analyst.Load();
            m_standardLog.Debug("Facility Analyst LOAD");
            if (isPipe)
            {
                secondPipes.UnionWith(analyst.FindConnectedEdgesFromEdges(new int[] { id }));
            }
            else
            {
                secondPipes.UnionWith(analyst.FindConnectedEdgesFromNodes(new int[] { id }));
            }
            secondPipes.ExceptWith(firstPipes);
            try
            {
                IList<int> secondCloseEdge = closeEdge.Keys.Intersect(secondPipes).ToList();
                foreach (int edgeId in secondCloseEdge)
                {
                    secondPipes.UnionWith(closeEdge[edgeId]);
                }
            }
            catch (Exception ex) { }
            m_standardLog.Debug("2차 배관 분석 완료");

            //insert resultvalve, resultpipe, resultbuilding
            DatasetVector targetPipe = targetDatasource.Datasets["RESULTPIPE"] as DatasetVector;
            DatasetVector targetValve = targetDatasource.Datasets["RESULTVALVE"] as DatasetVector;
            DatasetVector targetBuilding = targetDatasource.Datasets["RESULTBUILDING"] as DatasetVector;
            DatasetVector sourcePipe = networkDv;
            DatasetVector sourceValve = networkDv.ChildDataset as DatasetVector;
            DatasetVector sourceBuilding = sourceDatasource.Datasets["BUILDING"] as DatasetVector;
            InsertResult(targetPipe, sourcePipe, firstPipes, 1);
            InsertResult(targetPipe, sourcePipe, secondPipes, 2);
            InsertResult(targetValve, sourceValve, firstValves, 1);
            InsertResult(targetValve, sourceValve, secondValves, 2);
            //InsertResultBuilding(targetBuilding, sourceBuilding);
            //PipeBuildingGrouping();
            DataTable table = m_gridDataset.Tables["pipeStatistic"];
            table.Rows.Add(new object[] { m_pipeLength["Hlength"], m_pipeLength["Mlength"], m_pipeLength["Llength"], m_pipeLength["Tlength"] });
            table = m_gridDataset.Tables["buildingStatistic"];
            table.Rows.Add(new object[] { m_users["Tusers"], m_users["Musers"] });
            m_boundary = targetPipe.ComputeBounds();
            return true;
        }
        */
        private HashSet<int> GetClosingRegulator(FacilityAnalyst analyst, int[] rgltIds, HashSet<int> checkValves)
        {
            HashSet<int> closingNodes = new HashSet<int>();
            List<List<int>> rgltGroups = new List<List<int>>();
            foreach (int valveId in checkValves)
            {
                FacilityAnalystResult result = analyst.FindCriticalFacilitiesDownFromNode(rgltIds, valveId, true);
                if (result != null)
                {
                    foreach (int nodeId in result.Nodes)
                    {
                        List<int> rgltGroup = CheckLoopRegulator(analyst.AnalystSetting.NetworkDataset.ChildDataset, nodeId);
                        if(rgltGroup.Count > 0)
                        {
                            closingNodes.Add(nodeId);
                            rgltGroups.Add(rgltGroup);
                        }
                    }
                }
            }
            foreach(List<int> loopRglts in rgltGroups){
                bool loopFlag = true;
                foreach(int rgltId in loopRglts)
                {
                    if (!closingNodes.Contains(rgltId))
                    {
                        //다른 정압기를 통해 가스가 들어올수 있음!
                        loopFlag = false;
                        break;
                    }
                }
                if (loopFlag)
                {
                    foreach (int rgltId in loopRglts)
                    {
                        closingNodes.Remove(rgltId);
                    }
                }
            }
            return closingNodes;
        }
         
        private List<int> CheckLoopRegulator(DatasetVector dataset, int nodeId)
        {
            List<int> ret = new List<int>();
            QueryParameter param = new QueryParameter();
            //param.AttributeFilter = string.Format("PressureType = 'LP' AND Direction = 2 AND (SmFNode = {0} OR SmTNode = {0})", nodeId);
            param.AttributeFilter = string.Format("SMNODEID = {0}", nodeId);
            param.ResultFields = new string[] { "SMID", "SMNODEID", "RGLT_GROUP" };
            param.HasGeometry = false;
            param.CursorType = CursorType.Static;
            using (Recordset rs = dataset.Query(param))
            {
                if (!rs.IsEmpty)
                {
                    short rgltGroup = rs.GetInt16("RGLT_GROUP");
                    param = new QueryParameter();
                    param.AttributeFilter = string.Format("RGLT_GROUP = {0}", rgltGroup);
                    param.ResultFields = new string[] { "SMID", "SMNODEID" };
                    param.HasGeometry = false;
                    param.CursorType = CursorType.Static;
                    using (Recordset rs1 = dataset.Query(param))
                    {
                        while (!rs1.IsEOF)
                        {
                            ret.Add(rs1.GetInt32("SMNODEID"));
                            rs1.MoveNext();
                        }
                    }
                }
            }
            return ret;
        }

        public bool ExecuteFacilityAnalyst(string facility, int id, Point2D point, string regnCd)
        {
            bool isPipe = false;
            if (facility.Contains("배관"))
            {
                isPipe = true;
            }
            Datasource sourceDatasource = m_workspace.Datasources[m_properties.TargetDatasourceName];
            Datasource targetDatasource = m_workspace.Datasources[m_properties.SourceDatasourceName];
            DatasetVector networkDv = sourceDatasource.Datasets["network"] as DatasetVector;
            int[] valveIds = null;
            int[] barrierIds = null;
            int[] rgltIds = null;
            IDictionary<int, IList<int>> closeEdge = null;
            GetNodeIds(sourceDatasource, regnCd, out valveIds, out barrierIds, out closeEdge, out rgltIds);
            Point2Ds points = new Point2Ds();
            using (Recordset rs = (networkDv.ChildDataset as DatasetVector).Query("NodeType = 4", CursorType.Static))
            {
                int recordCnt = rs.RecordCount;
                valveIds = new int[recordCnt];
                for (int i = 0; i < recordCnt; i++)
                {
                    GeoPoint p = rs.GetGeometry() as GeoPoint;
                    if(!Geometrist.CanContain(p, new GeoPoint(point.X, point.Y)))
                    {
                        points.Add(new Point2D(p.X, p.Y));
                    }
                    valveIds[i] = rs.GetID();  
                    rs.MoveNext();
                }
            }
            FacilityAnalystSetting setting = GetFacilityAnalystSetting(networkDv, barrierIds);
            FacilityAnalyst analyst = new FacilityAnalyst();
            analyst.AnalystSetting = setting;
            analyst.Load();
            TransportationAnalystSetting transSetting = new TransportationAnalystSetting();

            transSetting.NetworkDataset = networkDv;
            transSetting.NodeIDField = "SmNodeID";
            transSetting.EdgeIDField = "SmID";
            transSetting.FNodeIDField = "SmFNode";
            transSetting.TNodeIDField = "SmTNode";
            WeightFieldInfos weightInfos = new WeightFieldInfos();
            WeightFieldInfo weightInfo = new WeightFieldInfo();
            weightInfo.FTWeightField = "SmLength";
            weightInfo.TFWeightField = "Smlength";
            weightInfo.Name = "length";
            weightInfos.Add(weightInfo);
            transSetting.WeightFieldInfos = weightInfos;
            TransportationAnalyst transAnal = new TransportationAnalyst();
            transAnal.AnalystSetting = transSetting;
            transAnal.Load();
            BurstAnalyseResult burstResult = null;
            FacilityAnalystResult facilityResult = null;
            
            HashSet<int> firstValves = new HashSet<int>();//상행
            HashSet<int> secondValves = new HashSet<int>();//하행
            HashSet<int> pipes = new HashSet<int>();
            if (isPipe)
            {
                burstResult = analyst.BurstAnalyseFromEdge(valveIds, id, true);
                facilityResult = analyst.TraceDownFromEdge(id, "length", true);
            }
            else
            {
                burstResult = analyst.BurstAnalyseFromNode(valveIds, id, true);
                facilityResult = analyst.TraceDownFromNode(id, "length", true);
            }
            TransportationAnalystParameter transParam = new TransportationAnalystParameter();
            transParam.Points = points;
            transParam.IsNodesReturn = true;
            transParam.IsEdgesReturn = true;
            transParam.BarrierNodes = barrierIds;
            transParam.WeightName = "length";
            TransportationAnalystResult transResult = transAnal.FindClosestFacility(transParam, point, 3, true, 0);

            if (burstResult != null)
            {
                firstValves.UnionWith(burstResult.CriticalNodes);
            }
            if (transResult != null)
            {
                int cnt = transResult.Edges.Length;
                for(int i = 0; i < cnt; i++)
                {
                    int[] edges = transResult.Edges[i];
                    int[] nodes = transResult.Nodes[i];
                    int edgeLastIdx = edges.Length - 1;
                    int nodeLastIdx = nodes.Length - 1;
                    int nodeId = -1;
                    if (nodeLastIdx > 0)
                    {
                        nodeId = nodes[nodeLastIdx];
                    }
                    int secondNode = 0;
                    using (Recordset rs = networkDv.Query(new int[] { edges[edgeLastIdx] }, CursorType.Static))
                    {
                        int fNode = rs.GetInt32("SmFNode");
                        secondNode = rs.GetInt32("SmTNode");
                        if (nodeId > 0)
                        {
                            if (nodeId != fNode)
                            {
                                secondNode = fNode;
                            }
                        }
                        else
                        {
                            using (Recordset rs1 = (networkDv.ChildDataset as DatasetVector).Query(string.Format("SmID in ({0},{1}) and NodeType = 4",fNode, secondNode), CursorType.Static))
                            {
                                secondNode = rs1.GetID();
                            }
                        }
                    }
                    secondValves.Add(secondNode);
                }
            }
            if(facilityResult != null)
            {
                pipes.UnionWith(facilityResult.Edges);
            }
            try
            {
                /*IDictionary<int, int> firstCloseEdge = closeEdge.Keys.Intersect(pipes).ToDictionary(t => t, t => closeEdge[t]);
                pipes.UnionWith(firstCloseEdge.Values);*/
                IList<int> firstCloseEdge = closeEdge.Keys.Intersect(pipes).ToList();
                foreach(int edgeId in firstCloseEdge)
                {
                    pipes.UnionWith(closeEdge[edgeId]);
                }
            }
            catch (Exception ex) { }

            DatasetVector targetPipe = targetDatasource.Datasets["RESULTPIPE"] as DatasetVector;
            DatasetVector targetValve = targetDatasource.Datasets["RESULTVALVE"] as DatasetVector;
            DatasetVector targetBuilding = targetDatasource.Datasets["RESULTBUILDING"] as DatasetVector;
            DatasetVector sourcePipe = networkDv;
            DatasetVector sourceValve = networkDv.ChildDataset as DatasetVector;
            DatasetVector sourceBuilding = sourceDatasource.Datasets["BUILDING"] as DatasetVector;
            InsertResult(targetPipe, sourcePipe, pipes, 1);
            InsertResult(targetValve, sourceValve, firstValves, 1);
            InsertResult(targetValve, sourceValve, secondValves, 2);
            DataTable secondTable = m_gridDataset.Tables["secondValve"];
            secondTable.Columns.Add(new DataColumn("order", typeof(int)));
            for(int i = 0; i < secondValves.Count; i++)
            {
                DataRow[] rows = secondTable.Select(string.Format("SourceID='{0}'", secondValves.ElementAt(i)));
                rows[0]["order"] = i;
            }
            secondTable.DefaultView.Sort = "order";

            InsertResultBuilding(targetBuilding, sourceBuilding);
            PipeBuildingGrouping();
            DataTable table = m_gridDataset.Tables["pipeStatistic"];
            table.Rows.Add(new object[] { m_pipeLength["Hlength"], m_pipeLength["Mlength"], m_pipeLength["Llength"], m_pipeLength["Tlength"] });
            table = m_gridDataset.Tables["buildingStatistic"];
            table.Rows.Add(new object[] { m_users["Tusers"], m_users["Musers"] });
            m_boundary = targetPipe.ComputeBounds();

            return true;
        }
       
        private void InsertResult(DatasetVector targetDataset, DatasetVector sourceDataset, HashSet<int> Ids, int shutorder)
        {
            string name = targetDataset.Name;
            if (name.Contains("PIPE"))
            {
                name = "pipe";
            }
            else
            {
                if(shutorder == 1)
                {
                    name = "firstValve";
                }
                else
                {
                    name = "secondValve";
                }
            }
            using (Recordset targetRs = targetDataset.GetRecordset(true, CursorType.Dynamic))
            {
                Recordset.BatchEditor editor = targetRs.Batch;
                editor.Begin();
              
                using (Recordset sourceRs = sourceDataset.Query(Ids.ToArray(), CursorType.Static))
                {
                    while (!sourceRs.IsEOF)
                    {
                        targetRs.AddNew(sourceRs.GetGeometry());
                        FieldInfos fieldInfos = targetRs.GetFieldInfos();
                        int cnt = fieldInfos.Count;
                        for (int i = 0; i < cnt; i++)
                        {
                            FieldInfo field = fieldInfos[i];
                            if (!field.IsSystemField && !field.Name.StartsWith("Sm") && !field.Name.Equals("ShutOrder"))
                            {
                                targetRs.SetObject(i, sourceRs.GetObject(field.Name));
                            }
                        }
                        targetRs.SetInt32("ShutOrder", shutorder);
                        if (name.Equals("pipe"))
                        {
                            string pipeId = sourceRs.GetString("PipeId");
                            string pipeType = sourceRs.GetString("PipeType");
                            if (pipeId != null && pipeId.Length > 0 && (pipeType.Equals("20") || pipeType.Equals("10")))
                            {
                                m_pipeIds.Add(int.Parse(pipeId));
                            }
                            double length = sourceRs.GetDouble("Length");
                            m_pipeLength["Tlength"] += length;
                            switch (sourceRs.GetString("PressureType"))
                            {
                                case "HP":
                                    m_pipeLength["Hlength"] += length;
                                    break;
                                case "MA":
                                    m_pipeLength["Mlength"] += length;
                                    break;
                                case "LP":
                                    m_pipeLength["Llength"] += length;
                                    break;
                            }
                        }
                        InsertDataTable(name, shutorder, sourceRs, targetRs.GetID());
                        sourceRs.MoveNext();
                    }
                }
                editor.Update();
            }            
        }

        private void InsertResultBuilding(DatasetVector targetDataset, DatasetVector sourceDataset)
        {
            using (Recordset targetRs = targetDataset.GetRecordset(true, CursorType.Dynamic))
            {
                Recordset.BatchEditor editor = targetRs.Batch;
                editor.Begin();
                QueryParameter param = new QueryParameter();
                param.CursorType = CursorType.Static;
                param.AttributeFilter = string.Format("PIPE_ID in ({0})", String.Join<int>(",", m_pipeIds));
                using (Recordset sourceRs = sourceDataset.Query(param))
                {
                    while (!sourceRs.IsEOF)
                    {
                        targetRs.AddNew(sourceRs.GetGeometry());
                        FieldInfos fieldInfos = targetRs.GetFieldInfos();
                        int cnt = fieldInfos.Count;
                        for (int i = 0; i < cnt; i++)
                        {
                            FieldInfo field = fieldInfos[i];
                            if (!field.IsSystemField && !field.Name.StartsWith("Sm") && !field.Name.Equals("ShutOrder"))
                            {
                                targetRs.SetObject(i, sourceRs.GetObject(field.Name));
                            }
                        }
                        if (InsertDataTable("building", 0, sourceRs, targetRs.GetID()))
                        {
                            m_users["Tusers"] += sourceRs.GetDouble("TOTAL_USER");
                            m_users["Musers"] += sourceRs.GetDouble("METER_USER");
                        }
                        sourceRs.MoveNext();
                    }
                }
                editor.Update();
            }
        }
        private void GetNodeIds(Datasource datasource, string regnCd, out int[] valveIds, out int[] barrierIds, out IDictionary<int, IList<int>> closeValveEdges, out int[] rgltIds)
        {
            DatasetVector idsDv = datasource.Datasets["ids"] as DatasetVector;
            using (Recordset idsRs = idsDv.GetRecordset(false, CursorType.Static))
            {
                valveIds = Util.ConvertToIntArray(idsRs.GetLongBinary("ids"));
                barrierIds = Util.ConvertToIntArray(idsRs.GetLongBinary("barrierIds"));
                closeValveEdges = Util.ConvertToDictionary(idsRs.GetLongBinary("closeValveEdges"));
                rgltIds = Util.ConvertToIntArray(idsRs.GetLongBinary("rgltIds"));
            }
        }
        private bool InsertDataTable(string tableName, int shutorder, Recordset rs, int SmID)
        {
            System.Data.DataTable table = m_gridDataset.Tables[tableName];
            DataRow row = table.NewRow();
            foreach(DataColumn col in table.Columns)
            {
                string colName = col.ColumnName;
                object obj = null;
                if (colName.Equals("ShutOrder"))
                {
                    obj = shutorder;
                }
                else if (colName.Equals("SmID"))
                {
                    obj = SmID;
                }else if (colName.Equals("SourceID"))
                {
                    obj = rs.GetID();
                }
                else { 
                    obj = rs.GetObject(colName);
                }
                if (colName.Equals("NodeType")){
                    if(obj.ToString() == "3")
                    {
                        obj = "밸브";
                    }
                    else
                    {
                        obj = "정압기";
                    }
                }
                row[colName] = obj;
            }
            bool isNewRow = true;
            if (tableName.Equals("pipe"))
            {
                string label = rs.GetString("Label");
                if (pipeLabel.ContainsKey(label))
                {
                    pipeLabel[label].Length += rs.GetDouble("Length");
                    pipeLabel[label].SmID += string.Format(",{0}", SmID);
                    isNewRow = false;
                }
                else
                {
                    pipeLabel.Add(label, new PipeProperties(SmID, rs.GetDouble("Length")));
                }
            }
            if (tableName.Equals("building"))
            {
                string code = rs.GetString("CODE");
                if (buildingCode.ContainsKey(code))
                {
                    buildingCode[code]+= string.Format(",{0}", SmID);
                    isNewRow = false;
                }
                else
                {
                    buildingCode.Add(code, SmID.ToString());
                }
            }
            if (isNewRow)
            {
                table.Rows.Add(row);
            }
            return isNewRow;
        }
        private void PipeBuildingGrouping()
        {
            DataTable pipeDt = m_gridDataset.Tables["pipe"];
            DataTable buildingDt = m_gridDataset.Tables["building"];
            foreach(DataRow row in pipeDt.Rows){
                string label = row["Label"].ToString();
                PipeProperties properties = pipeLabel[label];
                if (properties != null)
                {
                    row["SmID"] = properties.SmID;
                    row["Length"] = properties.Length;
                }
            }
            foreach (DataRow row in buildingDt.Rows)
            {
                string code = row["Code"].ToString();
                string smID = buildingCode[code];
                if (smID != null)
                {
                    row["SmID"] = smID;
                }
            }
        }
    }


}

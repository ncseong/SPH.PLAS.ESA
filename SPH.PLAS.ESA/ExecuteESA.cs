using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using SuperMap.Data;
using SuperMap.Analyst.NetworkAnalyst;
namespace SPH.PLAS.ESA
{
    public class ExecuteESA
    {
        private static ExecuteESA Instance = new ExecuteESA();
        private Workspace m_workspace;
        private ESAProperties m_properties;
        private IStandardLog m_standardLog;
        private readonly double m_tolerance = 0.005;
        private ExecuteESA(){}
        public static ExecuteESA GetInstace(ESAProperties esaProperties = default, IStandardLog standardLog = null)
        {
            Instance.SetProperties(esaProperties);
            Instance.SetLog(standardLog);
            WorkspaceConnectionInfo info = new WorkspaceConnectionInfo(Instance.m_properties.WorkspacePath);
            Instance.m_workspace = new Workspace();
            Instance.m_workspace.Open(info);
            return Instance;
        }
        /// <summary>
        /// C/S에서 ExecuteESA Instance 획득
        /// </summary>
        /// <param name="workspace">workspace</param>
        /// <param name="esaProperties">상황분석 설정 정보</param>
        /// <param name="standardLog">seriLog</param>
        /// <returns></returns>
        public static ExecuteESA GetInstance(Workspace workspace, ESAProperties esaProperties = default, IStandardLog standardLog = null)
        {
            Instance.SetProperties(esaProperties);
            Instance.SetLog(standardLog);
            Instance.m_workspace = workspace;
            return Instance;
        }
        private void SetProperties(ESAProperties esaProperties)
        {
            if (esaProperties.Equals(default(ESAProperties)))
            {
                string dir = AppDomain.CurrentDomain.BaseDirectory;
                NewtonJsonWrapper newtonJson = new NewtonJsonWrapper();
                m_properties = newtonJson.Read<ESAProperties>(string.Format("{0}esa.json",dir));
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
        /// <summary>
        /// NetworkAnalystSetting 생성
        /// </summary>
        /// <param name="networkDv"></param>
        /// <param name="barrierIds"></param>
        /// <returns></returns>
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
        /// <summary>
        /// 데이터소스 재오픈 - readonly 설정 변경
        /// </summary>
        /// <param name="datasource"></param>
        /// <param name="isReadOnly"></param>
        /// <returns></returns>
        private Datasource DatasourceReOpen(Datasource datasource, bool isReadOnly)
        {
            DatasourceConnectionInfo info = new DatasourceConnectionInfo(datasource.ConnectionInfo.Server, datasource.ConnectionInfo.Alias, null);
            info.EngineType = datasource.EngineType;
            info.IsReadOnly = isReadOnly;
            datasource.Close();
            datasource = m_workspace.Datasources.Open(info);
            return datasource;
        }
        /// <summary>
        /// 선택 시설물 정보 획득
        /// </summary>
        /// <param name="rsltNo"></param>
        /// <param name="sourceDatasource"></param>
        /// <param name="targetDatasource"></param>
        /// <param name="nodeIds"></param>
        /// <returns></returns>
        private List<SelectedFacility> GetSelectedFacility(string rsltNo, ref Datasource sourceDatasource, Datasource targetDatasource, ref NodeIds nodeIds)
        {
            sourceDatasource = DatasourceReOpen(sourceDatasource, false);
            List<SelectedFacility> selectedFacility = new List<SelectedFacility>();
            DatasetVector network = sourceDatasource.Datasets["network"] as DatasetVector;
            DatasetVector resultDv = targetDatasource.Datasets["ESA_RESULT"] as DatasetVector;
            DatasetVector virtualDv = targetDatasource.Datasets["ESA_VIRTUALVALVE"] as DatasetVector;
            string attributeFilter = string.Format("RSLT_NO = '{0}'", rsltNo);
            using (Recordset resultRs = resultDv.Query(attributeFilter, CursorType.Static))
            {
                while (!resultRs.IsEOF) { 
                    GeoPoint geometry = resultRs.GetGeometry() as GeoPoint;
                    SelectedFacility facility = new SelectedFacility();
                    facility.RsltNo = resultRs.GetString("RSLT_NO");
                    facility.Code = resultRs.GetString("Code");
                    facility.KeyValue = resultRs.GetString("KeyValue");
                    facility.Label = resultRs.GetString("Label");
                    facility.Name = resultRs.GetString("Name");
                    facility.ConstNo = resultRs.GetString("ConstNo");
                    facility.Geometry = new Point2D(geometry.X, geometry.Y);
                    List<int> virtualValveList = GetVirtualValves(rsltNo, network, virtualDv);
                    facility.VirtualValves = virtualValveList;
                    if (virtualValveList != null)
                    {
                        nodeIds.ValveNodeIds.UnionWith(facility.VirtualValves);
                    }
                    SetFacilityID(ref facility, nodeIds, network);
                    selectedFacility.Add(facility);
                    resultRs.MoveNext();
                }
            }
            
            sourceDatasource = DatasourceReOpen(sourceDatasource, true);
            return selectedFacility;
        }
        private SelectedFacility GetBarrierEdgeIDFromThrowValve(string rsltNo, Datasource sourceDatasource, Datasource targetDatasource, NodeIds nodeIds, string throwNo = null)
        {
            SelectedFacility selectedFacility = new SelectedFacility();
            selectedFacility.FacilityType = FacilityType.Node;
            DatasetVector network = sourceDatasource.Datasets["network"] as DatasetVector;
            DatasetVector node = network.ChildDataset;
            DatasetVector throwDv = targetDatasource.Datasets["ESA_THROWVALVE"] as DatasetVector;
            DatasetVector pipeDv = targetDatasource.Datasets["ESA_PIPELINE"] as DatasetVector;
            string attributeFilter = string.Format("RSLT_NO = '{0}'", rsltNo);
            QueryParameter param = new QueryParameter();
            param.CursorType = CursorType.Static;
            param.AttributeFilter = attributeFilter;
            param.OrderBy = new string[] { "SMID DESC" };
            using (Recordset throwRs = throwDv.Query(param))
            {                
                if (!throwRs.IsEmpty)
                {
                    Geometry throwPoint = null;
                    if (throwNo != null)
                    {
                        List<string> throwNoList = new List<string>();
                        throwNoList.Add(string.Format("'{0}'", rsltNo));
                        while (!throwRs.IsEOF)
                        {
                            string subThrowNo = throwRs.GetString("THROW_NO");
                            if (subThrowNo.Equals(throwNo))
                            {
                                throwPoint = throwRs.GetGeometry();
                            }
                            throwNoList.Add(string.Format("'{0}'", subThrowNo));
                            throwRs.MoveNext();
                        }
                        attributeFilter = string.Format("RSLT_NO in ({0})", string.Join(",", throwNoList));
                    }
                    else
                    {
                        throwPoint = throwRs.GetGeometry();
                    }                    
                    using (Recordset nodeRs = node.Query(throwPoint, m_tolerance, CursorType.Static))
                    {
                        int nodeID = nodeRs.GetInt32("SmNodeID");
                        short nodeType = nodeRs.GetInt16("NodeType");
                        int[] barrierNodeIds = nodeIds.BarrierNodeIds;
                        int lastIdx = barrierNodeIds.Length;
                        Array.Resize(ref barrierNodeIds, lastIdx + 1);
                        barrierNodeIds[lastIdx] = nodeID;
                        selectedFacility.BarrierNodeIds = barrierNodeIds;
                        using (Recordset networkRs = network.Query(string.Format("(SmTNode = {0} or SmFNode = {0})", nodeID), CursorType.Static))
                        {
                            while (!networkRs.IsEOF)
                            {
                                GeoLine line = networkRs.GetGeometry() as GeoLine;
                                int tNode = networkRs.GetInt32("SmTNode");
                                int fNode = networkRs.GetInt32("SmFNode");
                                Point2D nodePoint = line[0][0];
                                if (nodeID.Equals(tNode))
                                {
                                    selectedFacility.ID = fNode;
                                }
                                else
                                {
                                    selectedFacility.ID = tNode;
                                    int partCnt = line.PartCount - 1;
                                    int pointIdx = line[partCnt].Count - 1;
                                    nodePoint = line[partCnt][pointIdx];
                                }
                                using (Recordset pipeRs = pipeDv.Query(new GeoPoint(nodePoint), m_tolerance, attributeFilter, CursorType.Static))
                                {
                                    if (pipeRs.IsEmpty)
                                    {
                                        if (nodeType == 5 && Math.Round(line.Length, 3) > m_tolerance * 2)
                                        {
                                            selectedFacility.BarrierEdgeIds = new int[] { networkRs.GetInt32("SmEdgeID") };
                                        }
                                        break;
                                    }
                                }
                                networkRs.MoveNext();
                            }
                        }
                    }
                }
            }
            return selectedFacility;
        }
        /// <summary>
        /// 가상밸브 정보 획득
        /// </summary>
        /// <param name="rsltNo"></param>
        /// <param name="network"></param>
        /// <param name="virtualDv"></param>
        /// <returns></returns>
        private List<int> GetVirtualValves(string rsltNo, DatasetVector network, DatasetVector virtualDv)
        {
            List<int> virtualValves = null;
            using (Recordset virtualRs = virtualDv.Query(string.Format("RSLT_NO = '{0}'", rsltNo), CursorType.Static))
            {
                if (!virtualRs.IsEOF)
                {
                    virtualValves = SplitNetwork(virtualRs, network);
                }
            }
            return virtualValves;
        }
        /// <summary>
        /// 가상밸브 network dataset split
        /// </summary>
        /// <param name="virtualRs"></param>
        /// <param name="network"></param>
        /// <returns></returns>
        private List<int> SplitNetwork(Recordset virtualRs, DatasetVector network)
        {
            List<int> virtualValveIDList = new List<int>();
            while (!virtualRs.IsEOF)
            {
                GeoPoint point = virtualRs.GetGeometry() as GeoPoint;
                DatasetVector node = network.ChildDataset;
                int nodeID = 0;
                using(Recordset nodeRs = network.ChildDataset.Query(point, m_tolerance, CursorType.Dynamic))
                {
                    if (!nodeRs.IsEmpty)
                    {
                        nodeRs.Edit();
                        nodeRs.SetInt16("NodeType", 5);
                        nodeRs.Update();
                        virtualValveIDList.Add(nodeRs.GetInt32("SmNodeID"));
                        virtualRs.MoveNext();
                        continue;
                    }
                }
                using (Recordset networkRs = network.Query(point, m_tolerance, CursorType.Dynamic))
                {
                    if (networkRs.IsEOF)
                    {
                        return virtualValveIDList;
                    }
                    using (Recordset nodeRs = node.GetRecordset(true, CursorType.Dynamic))
                    {
                        nodeRs.AddNew(point);
                        nodeID = nodeRs.GetID();
                        virtualValveIDList.Add(nodeID);
                        Dictionary<string, object> dic = new Dictionary<string, object>();
                        dic.Add("SmNodeID", nodeID);
                        dic.Add("NodeType", 5);
                        nodeRs.SetValues(dic);
                        nodeRs.Update();
                    }
                
                    GeoLine line = networkRs.GetGeometry() as GeoLine;
                    FieldInfos fieldInfos = networkRs.GetFieldInfos();
                    Dictionary<string, object> attributes = new Dictionary<string, object>();
                    int smTNode = networkRs.GetInt32("SmTNode");
                    int smFNode = networkRs.GetInt32("SmFNode");
                    string[] systemFields = new string[] { "SmFNode", "SmTNode" };
                    for (int i = 0; i < fieldInfos.Count; i++)
                    {
                        if (!fieldInfos[i].IsSystemField && !fieldInfos[i].Name.StartsWith("Sm"))
                        {
                            attributes.Add(fieldInfos[i].Name, networkRs.GetObject(i));
                        }
                        if (systemFields.Contains(fieldInfos[i].Name))
                        {
                            attributes.Add(fieldInfos[i].Name, networkRs.GetObject(i));
                        }
                    }
                    networkRs.Delete();
                    GeoLine[] splitLineArr = Geometrist.SplitLine(line, point, 0.01);
                    for (int i = 0; i < splitLineArr.Length; i++)
                    {
                        GeoLine splitLine = splitLineArr[i];
                        if (Geometrist.IsIdentical(point, new GeoPoint(splitLine[0][0]), m_tolerance/2))
                        {
                            attributes["SmFNode"] = nodeID;
                            attributes["SmTNode"] = smTNode;
                        }
                        else
                        {
                            attributes["SmTNode"] = nodeID;
                            attributes["SmFNode"] = smFNode;
                        }

                        networkRs.AddNew(splitLine);                        
                        attributes["SmEdgeID"] = networkRs.GetID();
                        
                        networkRs.SetValues(attributes);
                        networkRs.Update();
                    }
                }
                virtualRs.MoveNext();
            }
            return virtualValveIDList;
        }
        /// <summary>
        /// 선택된 시설물의 network Dataset의 ID 획득
        /// </summary>
        /// <param name="selectedFacility"></param>
        /// <param name="nodeIds"></param>
        /// <param name="network"></param>
        private void SetFacilityID(ref SelectedFacility selectedFacility, NodeIds nodeIds, DatasetVector network)
        {
            DatasetVector node = network.ChildDataset;
            List<int> ids = new List<int>();
            GeoPoint point = new GeoPoint(selectedFacility.Geometry);
            using (Recordset nodeRs = node.Query(point, m_tolerance, CursorType.Static))
            {
                if (nodeRs.IsEOF)
                {
                    using(Recordset networkRs = network.Query(point, m_tolerance, CursorType.Static))
                    {
                        if (!networkRs.IsEOF)
                        {
                            bool flag = true;
                            int fNodeId = networkRs.GetInt32("SmFNode");
                            int tNodeId = networkRs.GetInt32("SmTNode");
                            if (nodeIds.ValveNodeIds.Contains(fNodeId))
                            {
                                selectedFacility.ID = tNodeId;
                                selectedFacility.FacilityType = FacilityType.Node;
                                flag = !flag;
                            }
                            if (nodeIds.ValveNodeIds.Contains(tNodeId))
                            {
                                selectedFacility.ID = fNodeId;
                                selectedFacility.FacilityType = FacilityType.Node;
                                flag = !flag;
                            }
                            if (flag)
                            {
                                selectedFacility.FacilityType = FacilityType.Edge;
                                selectedFacility.ID = networkRs.GetInt32("SmEdgeID");
                            }
                        }
                    }
                }
                else
                {
                    selectedFacility.FacilityType = FacilityType.Node;
                    selectedFacility.ID = nodeRs.GetInt32("SmNodeID");
                    ids.Add(selectedFacility.ID);
                }
            }
            List<int> barrierNodeIds = new List<int>();
            foreach(int barrierId in nodeIds.BarrierNodeIds)
            {
                if (!ids.Contains(barrierId))
                {
                    barrierNodeIds.Add(barrierId);
                }
            }
            selectedFacility.BarrierNodeIds = barrierNodeIds.ToArray();
        }
        /// <summary>
        /// 네트워크 데이터 생성시 생성된 node정보 획득(차단 nodeID, 닫힘밸브 ID, 정압기 ID, 수용가 ID)
        /// </summary>
        /// <param name="datasource"></param>
        /// <returns></returns>
        private NodeIds GetNodeIds(Datasource datasource)
        {
            DatasetVector idsDv = datasource.Datasets["ids"] as DatasetVector;
            NodeIds nodeIds = default;
            using (Recordset idsRs = idsDv.GetRecordset(false, CursorType.Static))
            {
                nodeIds = new NodeIds(Util.ConvertToHashSet(idsRs.GetLongBinary("ids")),
                    null,
                    Util.ConvertToIntArray(idsRs.GetLongBinary("barrierIds")),
                    Util.ConvertToIntArray(idsRs.GetLongBinary("rgltIds")),
                    Util.ConvertToHashSet(idsRs.GetLongBinary("customerNodes")));
            }
            return nodeIds;
        }
        /// <summary>
        /// 사고지점에 가스공급하는 정압기가 모두 연결되어 있지 않으면 barrier설정
        /// 해당 정압기 추출
        /// </summary>
        /// <param name="analyst"></param>
        /// <param name="rgltIds"></param>
        /// <param name="checkValves"></param>
        /// <returns></returns>
        private HashSet<int> GetClosingRegulator(FacilityAnalyst analyst, int[] rgltIds, HashSet<int> checkValves)
        {
            DatasetVector nodeDv = analyst.AnalystSetting.NetworkDataset.ChildDataset;
            HashSet<int> closingNodes = new HashSet<int>();
            HashSet<int> checkRgltNodes = new HashSet<int>();
            checkRgltNodes.UnionWith(checkValves);
            foreach (int valveId in checkValves)
            {
                FacilityAnalystResult result = analyst.FindCriticalFacilitiesDownFromNode(rgltIds, valveId, true);
                if (result != null)
                {
                    checkRgltNodes.UnionWith(result.Nodes);
                }
            }
            QueryParameter param = new QueryParameter();
            param.HasGeometry = false;
            param.ResultFields = new string[] { "SmID, SmNodeID" };
            param.CursorType = CursorType.Static;
            param.AttributeFilter = string.Format("RGLT_GROUP = (SELECT RGLT_GROUP FROM NETWORK_NODE WHERE NodeType = 4 AND SmNodeID in ({0}) GROUP BY RGLT_GROUP HAVING RGLT_CNT!=COUNT(*))", string.Join(",", checkRgltNodes));
            //param.GroupBy = new string[] { "SmNodeID HAVING RGLT_CNT != count(*)" };
            using(Recordset rs = nodeDv.Query(param))
            {
                while (!rs.IsEOF)
                {
                    closingNodes.Add(rs.GetInt32("SmNodeID"));
                    rs.MoveNext();
                }
            }
            return closingNodes;
        }
        /// <summary>
        /// 1차 밸브 이후 추가 2차 밸브를 찾음
        /// </summary>
        /// <param name="analyst"></param>
        /// <param name="valveIds"></param>
        /// <param name="firstPipes"></param>
        /// <param name="firstValves"></param>
        /// <returns></returns>
        private HashSet<int> SecondValveClassify(FacilityAnalyst analyst, int[] valveIds, HashSet<int> firstPipes, HashSet<int> firstValves)
        {
            HashSet<int> secondValves = new HashSet<int>();
            DatasetVector network = analyst.AnalystSetting.NetworkDataset;
            QueryParameter param = new QueryParameter();
            param.HasGeometry = false;
            param.CursorType = CursorType.Static;
            param.AttributeFilter = string.Format("SmEdgeID in ({0}) AND (SmFNode in ({1}) OR SmTNode in ({1}))", string.Join(",", firstPipes), string.Join(",",firstValves));
            using(Recordset rs = network.Query(param))
            {
                while (!rs.IsEOF)
                {
                    int tNode = rs.GetInt32("SmTNode");
                    int nodeID = rs.GetInt32("SmFNode");
                    if (firstValves.Contains(nodeID))
                    {
                        nodeID = tNode;
                    }
                    BurstAnalyseResult result = analyst.BurstAnalyseFromNode(valveIds, nodeID, true);
                    secondValves.UnionWith(result.CriticalNodes);
                    secondValves.UnionWith(result.NormalNodes);
                    firstPipes.UnionWith(result.Edges);
                    rs.MoveNext();
                }
                rs.Dispose();
            }
            return secondValves;
        }
        /// <summary>
        /// 특정 밸브가 잠김으로 인하여 가스가 공급이 불가능한 밸브 찾기
        /// </summary>
        /// <param name="analyst"></param>
        /// <param name="valveIds"></param>
        /// <param name="firstValves"></param>
        /// <returns></returns>
        private List<int> GetFirstToSecondValve(FacilityAnalyst analyst, SelectedFacility selectedFacility, int[] rgltIds, HashSet<int> firstValves)
        {
            List<int> result = firstValves.ToList();
            FacilityAnalystResult facilityResult = null;
            if (selectedFacility.FacilityType.Equals(FacilityType.Node))
            {
                facilityResult = analyst.FindCriticalFacilitiesUpFromNode(rgltIds, selectedFacility.ID, true);
            }
            else
            {
                facilityResult = analyst.FindCriticalFacilitiesUpFromEdge(rgltIds, selectedFacility.ID, true);
            }
            if (facilityResult == null)
            {
                return new List<int>();
            }
            int[] checkRgltArr = facilityResult.Nodes;
            int[] valveIds = firstValves.ToArray();
            foreach(int nodeId in checkRgltArr)
            {
                if (!firstValves.Contains(nodeId))
                {
                    facilityResult = analyst.FindCriticalFacilitiesDownFromNode(valveIds, nodeId, true);
                    foreach (int id in facilityResult.Nodes)
                    {
                        result.Remove(id);
                    }
                }
                else
                {
                    //1차 차단이 정압기 인경우 1차 차단 유지!
                    result.Remove(nodeId);
                }
            }
            return result;
        }
        private bool ExecuteAnalystAndSaveData(FacilityAnalyst analyst, string rsltNo, NodeIds nodeIds, Datasource targetDatasource, DatasetVector network, SelectedFacility selectedFacility, bool isDelete)
        {
            int[] valveIds = nodeIds.ValveNodeIds.ToArray<int>();

            FacilityAnalystResult facilityResult = null;
            BurstAnalyseResult burstResult = null;
            HashSet<int> firstValves = new HashSet<int>();            
            HashSet<int> secondValves = new HashSet<int>();
            HashSet<int> firstPipes = new HashSet<int>();
            if(selectedFacility.BarrierEdgeIds != null)
            {
                firstPipes.UnionWith(selectedFacility.BarrierEdgeIds);
            }
            HashSet<int> secondPipes = new HashSet<int>();
            HashSet<int> buildings = new HashSet<int>();
            if (selectedFacility.FacilityType.Equals(FacilityType.Node))
            {
                facilityResult = analyst.FindCriticalFacilitiesUpFromNode(valveIds, selectedFacility.ID, true);
                burstResult = analyst.BurstAnalyseFromNode(valveIds, selectedFacility.ID, true);
            }
            else
            {
                facilityResult = analyst.FindCriticalFacilitiesUpFromEdge(valveIds, selectedFacility.ID, true);
                burstResult = analyst.BurstAnalyseFromEdge(valveIds, selectedFacility.ID, true);
            }
            if (facilityResult == null)
            {
                //차단 밸브 없음!
                return false;
            }

            m_standardLog.Information("1차 밸브 분석 완료");
            //1차 차단 밸브, 정압기
            firstValves.UnionWith(facilityResult.Nodes);
            //loop관련 RING에 사고지점, 1차밸브가 다있는경우처리
            List<int> exceptValves = GetFirstToSecondValve(analyst, selectedFacility, nodeIds.RgltNodeIds, firstValves);
            firstValves.ExceptWith(exceptValves);
            //1차 차단 배관
            firstPipes.UnionWith(burstResult.Edges);
            //2차 차단 밸브, 정압기
            secondValves.UnionWith(SecondValveClassify(analyst, valveIds, firstPipes, firstValves));//1차 이후 누락된 2차 밸브
            secondValves.UnionWith(burstResult.CriticalNodes);
            secondValves.UnionWith(burstResult.NormalNodes);
            
            //2차 차단 밸브, 정압기
            secondValves.ExceptWith(firstValves);

            FacilityAnalystSetting setting = analyst.AnalystSetting;
            HashSet<int> barrierNodes = GetClosingRegulator(analyst, nodeIds.RgltNodeIds, secondValves);
            barrierNodes.UnionWith(setting.BarrierNodes);
            barrierNodes.UnionWith(firstValves);            
            analyst.Dispose();            
            setting.BarrierNodes = null;
            analyst = new FacilityAnalyst();
            setting.BarrierNodes = barrierNodes.ToArray();
            analyst.AnalystSetting = setting;
            analyst.Load();
            foreach (int nodeID in secondValves)
            {
                facilityResult = analyst.TraceDownFromNode(nodeID, "length", true);
                if (facilityResult != null)
                {
                    secondPipes.UnionWith(facilityResult.Edges);
                    buildings.UnionWith(facilityResult.Nodes);
                }
            }
            m_standardLog.Information("2차 배관 분석 완료");
            secondPipes.ExceptWith(firstPipes);
            DatasetVector targetPipe = targetDatasource.Datasets["ESA_PIPELINE"] as DatasetVector;
            DatasetVector targetValve = targetDatasource.Datasets["ESA_VALVE"] as DatasetVector;
            DatasetVector targetBuilding = targetDatasource.Datasets["ESA_BUILDING"] as DatasetVector;
            DatasetVector sourcePipe = network;
            DatasetVector sourceValve = network.ChildDataset as DatasetVector;
            DatasetVector sourceBuilding = network.Datasource.Datasets["CustomerNode"] as DatasetVector;
            m_standardLog.Information("1차 밸브, 정압기 입력...");
            InsertResult(rsltNo, targetValve, sourceValve, firstValves, 1, isDelete);
            m_standardLog.Information("2차 밸브, 정압기 입력...");
            InsertResult(rsltNo, targetValve, sourceValve, secondValves, 2, isDelete);
            m_standardLog.Information("1차 배관 입력...");
            InsertResult(rsltNo, targetPipe, sourcePipe, firstPipes, 1, isDelete, buildings);
            buildings.IntersectWith(nodeIds.CustomerNodeIds);
            m_standardLog.Information("2차 배관 입력...");
            InsertResult(rsltNo, targetPipe, sourcePipe, secondPipes, 2, isDelete);
            m_standardLog.Information("공급중단 수용가 입력...");
            InsertCustomerResult(rsltNo, targetBuilding, sourceBuilding, buildings, isDelete);
            analyst.Dispose();
            m_standardLog.Information("분석끝");
            return true;
        }
        public bool ExecuteSituationAnalyst(string rsltNo)
        {
            bool result = true;
            m_standardLog.Information(string.Format("상황분석 시작 -- RSLTNO = {0}",rsltNo));
            Datasource sourceDatasource = m_workspace.Datasources[m_properties.TargetDatasourceName];
            Datasource targetDatasource = m_workspace.Datasources[m_properties.SourceDatasourceName];
            NodeIds nodeIds = GetNodeIds(sourceDatasource);
            List<SelectedFacility> selectedFacilityList = GetSelectedFacility(rsltNo, ref sourceDatasource, targetDatasource, ref nodeIds);
            
            DatasetVector network = sourceDatasource.Datasets["network"] as DatasetVector;
            foreach(SelectedFacility selectedFacility in selectedFacilityList)
            {
                FacilityAnalystSetting setting = GetFacilityAnalystSetting(network, selectedFacility.BarrierNodeIds);
                FacilityAnalyst analyst = new FacilityAnalyst();
                analyst.AnalystSetting = setting;
                analyst.Load();
                m_standardLog.Debug("Network Anlayst Load");
                result = ExecuteAnalystAndSaveData(analyst, rsltNo, nodeIds, targetDatasource, network, selectedFacility, true);                
            }
            (targetDatasource.Datasets["ESA_RESULT"] as DatasetVector).UpdateField("RSLT_STATUS", 1, string.Format("RSLT_NO = '{0}'", rsltNo));
            return result;
        }
        public bool ExecuteThrowValve(string rsltNo)
        {
            bool result = false;
            m_standardLog.Information(string.Format("3차밸브 분석 시작 -- RSLTNO = {0}", rsltNo));
            Datasource sourceDatasource = m_workspace.Datasources[m_properties.TargetDatasourceName];
            Datasource targetDatasource = m_workspace.Datasources[m_properties.SourceDatasourceName];
            NodeIds nodeIds = GetNodeIds(sourceDatasource);
            SelectedFacility selectedFacility =  GetBarrierEdgeIDFromThrowValve(rsltNo, sourceDatasource, targetDatasource, nodeIds);
            DatasetVector network = sourceDatasource.Datasets["network"] as DatasetVector;

            FacilityAnalystSetting setting = GetFacilityAnalystSetting(network, selectedFacility.BarrierNodeIds);
            FacilityAnalyst analyst = new FacilityAnalyst();
            analyst.AnalystSetting = setting;
            analyst.Load();
            m_standardLog.Debug("Network Anlayst Load");
            result = ExecuteAnalystAndSaveData(analyst, rsltNo, nodeIds, targetDatasource, network, selectedFacility, false);
            return true;
        }
        public bool ExecuteThrowValveFromWeb(string rsltNo, string throwNo)
        {
            bool result = false;
            m_standardLog.Information(string.Format("3차밸브 분석 시작 -- rsltNo = {0} throwNo = {1}", rsltNo, throwNo));
            Datasource sourceDatasource = m_workspace.Datasources[m_properties.TargetDatasourceName];
            Datasource targetDatasource = m_workspace.Datasources[m_properties.SourceDatasourceName];
            NodeIds nodeIds = GetNodeIds(sourceDatasource);
            SelectedFacility selectedFacility = GetBarrierEdgeIDFromThrowValve(rsltNo, sourceDatasource, targetDatasource, nodeIds, throwNo);
            DatasetVector network = sourceDatasource.Datasets["network"] as DatasetVector;

            FacilityAnalystSetting setting = GetFacilityAnalystSetting(network, selectedFacility.BarrierNodeIds);
            FacilityAnalyst analyst = new FacilityAnalyst();
            analyst.AnalystSetting = setting;
            analyst.Load();
            m_standardLog.Debug("Network Anlayst Load");
            result = ExecuteAnalystAndSaveData(analyst, throwNo, nodeIds, targetDatasource, network, selectedFacility, false);
            (targetDatasource.Datasets["ESA_THROWVALVE"] as DatasetVector).UpdateField("RSLT_STATUS", 1, string.Format("THROW_NO = '{0}'", throwNo));
            return true;
        }
        private void SetFieldValues(Recordset targetRs, Recordset sourceRs)
        {
            FieldInfos fieldInfos = targetRs.GetFieldInfos();
            int cnt = fieldInfos.Count;
            for (int i = 0; i < cnt; i++)
            {
                FieldInfo field = fieldInfos[i];
                if (!field.IsSystemField && !field.Name.StartsWith("Sm") && !field.Name.ToUpper().Equals("SHUTORDER") && !field.Name.Equals("RSLT_NO"))
                {
                    targetRs.SetObject(i, sourceRs.GetObject(field.Name));
                }
            }
        }
        private void InsertResult(string rsltNo, DatasetVector targetDataset, DatasetVector sourceDataset, HashSet<int> Ids, int shutorder, bool isDelete = false, HashSet<int> firstCustomerNodeIds = null)
        {
            using (Recordset targetRs = targetDataset.Query(string.Format("RSLT_NO = '{0}' AND ShutOrder = {1}", rsltNo, shutorder), CursorType.Dynamic))
            {
                if (isDelete)
                {
                    targetRs.DeleteAll();
                }
                Recordset.BatchEditor editor = targetRs.Batch;
                editor.Begin();
                using (Recordset sourceRs = sourceDataset.Query(Ids.ToArray(), CursorType.Static))
                {
                    while (!sourceRs.IsEOF)
                    {
                        if (targetDataset.Name.Contains("PIPE"))
                        {
                            if(firstCustomerNodeIds != null)
                            {
                                firstCustomerNodeIds.Add(sourceRs.GetInt32("SmTNode"));
                                firstCustomerNodeIds.Add(sourceRs.GetInt32("SmFNode"));
                            }
                            GeoLine line = sourceRs.GetGeometry() as GeoLine;
                            if (Math.Round(line.Length,3) <= m_tolerance * 2)
                            {
                                sourceRs.MoveNext();
                                continue;
                            }
                        }
                        targetRs.AddNew(sourceRs.GetGeometry());
                        targetRs.SetString("RSLT_NO", rsltNo);
                        targetRs.SetInt32("ShutOrder", shutorder);
                        SetFieldValues(targetRs, sourceRs);
                        sourceRs.MoveNext();
                    }
                }
                editor.Update();
            }
        }
        private void InsertCustomerResult(string rsltNo, DatasetVector targetDataset, DatasetVector sourceDataset, HashSet<int> Ids, bool isDelete)
        {
            string nodeIds = string.Join(",", Ids);
            QueryParameter param = new QueryParameter();
            param.HasGeometry = false;
            param.CursorType = CursorType.Static;
            param.AttributeFilter = string.Format("NodeID in ({0})", nodeIds);
            param.ResultFields = new string[] { "Code", "MAX(cnt) max_cnt", "count(*) tot_cnt" };
            param.GroupBy = new string[] { "Code" };
            Dictionary<string, bool> buildCodeDic = new Dictionary<string, bool>();
            StringBuilder filter = new StringBuilder();
            filter.Append("Code in (");
            using(Recordset rs = sourceDataset.Query(param))
            {
                bool flag = true;
                while (!rs.IsEOF)
                {
                    string code = rs.GetString("Code");
                    buildCodeDic.Add(code, rs.GetInt32("max_cnt").Equals(rs.GetInt32("tot_cnt")));
                    if (flag)
                    {
                        flag = false;
                    }
                    else
                    {
                        filter.Append(",");
                    }
                    filter.Append("'").Append(code).Append("'");
                    rs.MoveNext();
                }
            }
            filter.Append(")");
            DatasetVector building = sourceDataset.Datasource.Datasets["BUILDING"] as DatasetVector;
            param = new QueryParameter();
            param.CursorType = CursorType.Static;
            param.AttributeFilter = filter.ToString();
            using (Recordset targetRs = targetDataset.Query(string.Format("RSLT_NO = '{0}'", rsltNo), CursorType.Dynamic))
            {
                if (isDelete)
                {
                    targetRs.DeleteAll();
                }
                Recordset.BatchEditor editor = targetRs.Batch;
                editor.Begin();
                using (Recordset sourceRs = building.Query(param))
                {
                    while (!sourceRs.IsEOF)
                    {
                        targetRs.AddNew(sourceRs.GetGeometry());
                        targetRs.SetString("RSLT_NO", rsltNo);
                        SetFieldValues(targetRs, sourceRs);
                        //2개 배관 인입, 공급중단 여부
                        string type = sourceRs.GetString("TYPE");
                        string code = sourceRs.GetString("Code");
                        type = string.Format("{0}_{1}", buildCodeDic[code]?"O":"X", type);
                        targetRs.SetString("Type", type);
                        sourceRs.MoveNext();
                    }
                }
                editor.Update();
            }
        }

        public void TruncateResultDatasets(string rsltNo)
        {
            Datasource ds = m_workspace.Datasources[m_properties.SourceDatasourceName];
            DatasetVector valve = ds.Datasets["ESA_VALVE"] as DatasetVector;
            DeleteDataset(rsltNo, valve);
            DatasetVector pipe = ds.Datasets["ESA_PIPELINE"] as DatasetVector;
            DeleteDataset(rsltNo, pipe);
            DatasetVector building = ds.Datasets["ESA_BUILDING"] as DatasetVector;
            DeleteDataset(rsltNo, building);
        }
        private void DeleteDataset(string rsltNo, DatasetVector vector)
        {
            using(Recordset rs = vector.Query(string.Format("RSLT_NO='{0}'", rsltNo), CursorType.Dynamic))
            {
                rs.DeleteAll();
            }
        }
    }
}

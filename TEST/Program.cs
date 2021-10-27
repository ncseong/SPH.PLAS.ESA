using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using SuperMap.Data;
using SuperMap.Analyst.NetworkAnalyst;
using SuperMap.Data.Topology;
using SuperMap.Analyst.SpatialAnalyst;

using SPH.PLAS.ESA;
namespace TEST
{
    class Program
    {
        static void Main(string[] args)
        {
            Workspace work = new Workspace();
            DatasourceConnectionInfo info = new DatasourceConnectionInfo();
            info.Server = @"C:/SuperMap/udb/emg/20210908144056.udbx";
            info.EngineType = EngineType.UDBX;
            Datasource ds = work.Datasources.Open(info);
            DatasetVector network = ds.Datasets["network"] as DatasetVector;
            DatasetVector node = network.ChildDataset;
            using (Recordset nodeRs = node.Query("RGLT_GROUP>0", CursorType.Static))
            {
                while (!nodeRs.IsEOF)
                {
                    int nodeID = nodeRs.GetInt32("SmNodeID");
                    using(Recordset networkRs = network.Query(string.Format("(SmTNode = {0} or SmFNode = {0}) AND PressureType='중압' AND Direction = 2", nodeID),CursorType.Static))
                    {
                        while (!networkRs.IsEOF)
                        {
                            Console.WriteLine(networkRs.GetInt32("SmEdgeID"));
                            networkRs.MoveNext();
                        }
                    }
                    nodeRs.MoveNext();
                }
            }
                /*DatasetVector idsDv = ds.Datasets["ids"] as DatasetVector;
                int[] rgltIds = null;
                using (Recordset idsRs = idsDv.GetRecordset(false, CursorType.Static))
                {
                    rgltIds = Util.ConvertToIntArray(idsRs.GetLongBinary("rgltIds"));                
                }
                SetLoopRegulator(network, rgltIds);*/
                ds.Close();
            work.Close();
        }
        private static FacilityAnalystSetting GetFacilityAnalystSetting(DatasetVector network)
        {
            FacilityAnalystSetting setting = new FacilityAnalystSetting();
            setting.NetworkDataset = network;
            setting.NodeIDField = "SmNodeID";
            setting.EdgeIDField = "SmEdgeID";
            setting.FNodeIDField = "SmFNode";
            setting.TNodeIDField = "SmTNode";
            setting.DirectionField = "Direction";
            setting.Tolerance = 0.05;

            WeightFieldInfo fieldInfo = new WeightFieldInfo();
            fieldInfo.Name = "length";
            fieldInfo.FTWeightField = "SmLength";
            fieldInfo.TFWeightField = "SmLength";
            WeightFieldInfos fieldInfos = new WeightFieldInfos();
            fieldInfos.Add(fieldInfo);
            setting.WeightFieldInfos = fieldInfos;

            return setting;
        }
        private static void SetLoopRegulator(DatasetVector network, int[] rgltIds)
        {
            FieldInfo fieldInfo = network.ChildDataset.FieldInfos["RGLT_GROUP"];
            if (fieldInfo == null)
            {
                FieldInfo field = new FieldInfo("RGLT_GROUP", FieldType.Int16);
                network.ChildDataset.FieldInfos.Add(field);
                field = new FieldInfo("RGLT_CNT", FieldType.Int16);
                network.ChildDataset.FieldInfos.Add(field);
            }
            FacilityAnalyst analyst = new FacilityAnalyst();
            analyst.AnalystSetting = GetFacilityAnalystSetting(network);
            analyst.Load();
            List<HashSet<int>> loopRegulatorIds = new List<HashSet<int>>();
            foreach (int rgltId in rgltIds)
            {
                using(Recordset rs = network.ChildDataset.Query(string.Format("Code = '지구정압기' AND SmNodeID = {0}", rgltId),CursorType.Static)) {
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
            analyst.Dispose();
            loopRegulatorIds.Clear();
        }
        public static void CleanUDB(DateTime now, string path, int udbLifePeriod, string name)
        {
            string today = now.ToString("yyyyMMdd");
            if (!path.EndsWith("\\"))
            {
                path += "\\";
            }
            string backupPath = string.Format("{0}backup\\{1}\\{2}", path, name, today);
            if (!Directory.Exists(backupPath))
            {
                Directory.CreateDirectory(backupPath);
            }
            path += name;
            string[] files = Directory.GetFiles(path, "*.udbx", SearchOption.AllDirectories);
            foreach (string filePath in files)
            {
                if (File.GetCreationTime(filePath) < now)
                {
                    string fileName = filePath.Substring(filePath.LastIndexOf("\\"));
                    try
                    {
                        string toFile = string.Format("{0}{1}", backupPath, fileName);
                        Console.WriteLine(filePath+" / /"+ toFile);
                        File.Move(filePath, toFile);
                    }
                    catch (Exception e)
                    {
                        throw e;
                    }
                }
            }


            string[] dirs = Directory.GetDirectories(backupPath.Substring(0, backupPath.LastIndexOf("\\")));
            foreach (string dir in dirs)
            {
                if (Directory.GetCreationTime(dir).AddDays(udbLifePeriod) <= now)
                {
                    Directory.Delete(dir, true);
                }
            }
        }
        private static void AppendToNetwork(Datasource ds)
        {
            DatasetVector dv = ds.Datasets["network1"] as DatasetVector;
            bool e = NetworkBuilder.AppendToNetwork(dv, new DatasetVector[] { ds.Datasets["point1"] as DatasetVector });

        }
        private static void SplitNetwork(Datasource ds)
        {
            Point2D point2D = new Point2D(155808.703655085,  473786.609535512);
            GeoPoint point = new GeoPoint(point2D);
            DatasetVector network = ds.Datasets["network"] as DatasetVector;
            DatasetVector node = network.ChildDataset;
            int nodeID = 0;
            using (Recordset nodeRs = node.GetRecordset(true, CursorType.Dynamic))
            {
                bool aa = nodeRs.AddNew(point);
                nodeID = nodeRs.GetID();
                Dictionary<string,object> dic = new Dictionary<string, object>();
                dic.Add("SmNodeID", nodeID);

                nodeRs.SetValues(dic);
                nodeRs.Update();
            }
            using (Recordset networkRs = network.Query(point, 0.01, CursorType.Dynamic))
            {
                GeoLine line = networkRs.GetGeometry() as GeoLine;
                FieldInfos fieldInfos = networkRs.GetFieldInfos();
                Dictionary<string, object> attributes = new Dictionary<string, object>();
                int smTNode = networkRs.GetInt32("SmTNode");
                int smFNode = networkRs.GetInt32("SmFNode");
                string[] systemFields = new string[] { "SmFNode", "SmTNode", "SmEdgeID" };
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
                int edgeID = networkRs.GetID();
                networkRs.Delete();
                GeoLine[] splitLineArr = Geometrist.SplitLine(line, point, 0.01);
                for (int i = 0; i < splitLineArr.Length; i++)
                {
                    GeoLine splitLine = splitLineArr[i];
                    if (point2D.Equals(splitLine[0][0]))
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
                    if (i != 0)
                    {
                        attributes["SmEdgeID"] = networkRs.GetID();
                    }                    
                    networkRs.SetValues(attributes);
                    networkRs.Update();
                }
            }
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
        private void SpatialQuery(Datasource ds)
        {
            HashSet<int> ids = new HashSet<int>();
            ids.Add(1869);
            ids.Add(1889);
            DatasetVector node = ds.Datasets["CustomerNode"] as DatasetVector;
            DatasetVector build = ds.Datasets["BUILDING"] as DatasetVector;
            QueryParameter param = new QueryParameter();
            param.HasGeometry = false;
            param.CursorType = CursorType.Static;
            param.AttributeFilter = string.Format("SmNodeID_1 in ({0})", string.Join(",", ids));
            param.ResultFields = new string[] { "Code" };
            param.GroupBy = new string[] { "Code HAVING cnt=COUNT(*)" };
            StringBuilder filter = new StringBuilder();
            filter.Append("Code in (");
            using (Recordset rs = node.Query(param))
            {
                bool flag = true;
                while (!rs.IsEOF)
                {
                    if (flag)
                    {
                        flag = false;
                    }
                    else
                    {
                        filter.Append(",");
                    }
                    filter.Append("'").Append(rs.GetString("Code")).Append("'");
                    rs.MoveNext();
                }
            }
            filter.Append(")");
            param = new QueryParameter();
            param.CursorType = CursorType.Static;
            param.AttributeFilter = filter.ToString();
            using (Recordset rs1 = build.Query(param))
            {
                int cnt = rs1.RecordCount;
            }
        }
        private static void Intersect(Datasource ds)
        {
            DatasetVector pipe = ds.Datasets["network"] as DatasetVector;
            DatasetVector node = pipe.ChildDataset;
            DatasetVector building = ds.Datasets["BUILDING"] as DatasetVector;
            ds.Datasets.Delete("CustomerNode");
            DatasetVectorInfo info = new DatasetVectorInfo("CustomerNode", DatasetType.Tabular);
            DatasetVector dv = ds.Datasets.Create(info);
            dv.FieldInfos.Add(new FieldInfo("cnt", FieldType.Int32));
            dv.FieldInfos.Add(new FieldInfo("buildCode", FieldType.WText));
            dv.FieldInfos.Add(new FieldInfo("nodeID", FieldType.Int32));
            dv.PrjCoordSys = ds.PrjCoordSys;
            using (Recordset nodeRs = node.Query("NodeType = 2", CursorType.Static))
            {
                using (Recordset buildingRs = building.GetRecordset(false, CursorType.Static))
                {
                    OverlayAnalystParameter param = new OverlayAnalystParameter();
                    param.Tolerance = 0.005;
                    param.OperationRetainedFields = new string[] { "Code" };
                    param.SourceRetainedFields = new string[] { "SmNodeID" };
                    OverlayAnalyst.Intersect(nodeRs, buildingRs, dv, param);
                }
            }
            dv.BuildFieldIndex(new string[] { "SmNodeID_1" }, "idx_customernode_nodeid");
            dv.BuildFieldIndex(new string[] { "Code" }, "idx_customernode_code");

            
        }
        private static void ExecuteTopo(Datasource ds)
        {
            DatasetVector pipe = ds.Datasets["network"] as DatasetVector;
            HashSet<int> dangleid = new HashSet<int>();
            HashSet<int> connectedid = new HashSet<int>();
            using (Recordset rs = pipe.GetRecordset(false, CursorType.Static))
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
            DatasetVector node = pipe.ChildDataset;
            using(Recordset rs = node.GetRecordset(false, CursorType.Dynamic))
            {
                Recordset.BatchEditor editor = rs.Batch;
                editor.Begin();
                foreach(int nodeid in dangleid)
                {
                    rs.SeekID(nodeid);
                    rs.SetInt16("NodeType", 2);
                }
                editor.Update();
            }
        }

        private static void SetRegulatorDirection(Datasource ds)
        {
            DatasetVector network = ds.Datasets["network"] as DatasetVector;
            DatasetVector node = network.ChildDataset;
            QueryParameter param = new QueryParameter();
            param.ResultFields = new string[] { "SmEdgeID", "SmFNode", "SmTNode", "PressureType", "Direction" };
            param.HasGeometry = false;
            param.CursorType = CursorType.Dynamic;
            Dictionary<int, int> valveDic = new Dictionary<int, int>();
            using(Recordset nodeRs = node.Query("NodeType = 3", CursorType.Static))
            {
                while (!nodeRs.IsEOF)
                {
                    valveDic.Add(nodeRs.GetInt32("SmNodeID"), 0);
                    nodeRs.MoveNext();
                }
            }
            using (Recordset nodeRs = node.Query("Code in ('지역정압기','구역형압력조정기')", CursorType.Static))
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

        private static void ForwardToLoop(DatasetVector network, QueryParameter param, int nodeid, Dictionary<int,int> valveDic)
        {
            if (!valveDic.ContainsKey(nodeid))
            {
                using (Recordset networkRs = network.Query(param))
                {
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

        private static void TopoPreprocess(DatasetVector[] datasets)
        {
            //TopologyValidator.PreprocessVertexSnap(datasets, VertexSnapType.NodeSnapNodes, 0.05, 0.05);
            //TopologyValidator.PreprocessAdjustVertex(datasets, 0.05);
            TopologyPreprocessOptions options = new TopologyPreprocessOptions();
            options.AreVertexesSnapped = true;
            options.AreVertexAdjusted = false;
            TopologyValidator.Preprocess(datasets, new int[] { 0,0,2 }, options, 0.05);
        }

        private static void AddFacilityDivideJunction(DatasetVector pipeDataset, DatasetVector pointDataset)
        {
            List<Geometry> geoms = new List<Geometry>();
            using (Recordset pointRs = pointDataset.GetRecordset(false, CursorType.Dynamic))
            {
                Point2D point = pointRs.GetGeometry().InnerPoint;
                while (!pointRs.IsEOF)
                {
                    QueryParameter param = new QueryParameter();
                    param.SpatialQueryMode = SpatialQueryMode.Intersect;
                    param.SpatialQueryObject = point;
                    param.CursorType = CursorType.Static;
                    GeoLine pipeLine = new GeoLine();
                    using (Recordset rs = pipeDataset.Query(param))
                    {
                        while (!rs.IsEOF)
                        {
                            GeoLine line = rs.GetGeometry() as GeoLine;
                            for(int i = 0; i < line.PartCount; i++)
                            {
                                pipeLine = Geometrist.Union(pipeLine, new GeoLine(line[i])) as GeoLine;
                            }
                            rs.MoveNext();
                        }
                    }
                    Point2Ds points = pipeLine[0];
                    for(int i = 0; i < points.Count; i++)
                    {
                        if (points[i].Equals(point))
                        {
                            Point2D firPoint = points[i - 1];
                            Point2D secPoint = points[i];
                            Point2D thrPoint = points[i + 1];
                            geoms.Add(GetDividing(secPoint, firPoint));
                            geoms.Add(GetDividing(secPoint, thrPoint));
                            break;
                        }
                    }
                    pointRs.MoveNext();
                }
                pointRs.AddNew(geoms[0]);
                pointRs.Update();
                pointRs.AddNew(geoms[1]);
                pointRs.Update();
            }
        }

        private static GeoPoint GetDividing(Point2D fromPoint, Point2D toPoint)
        {
            GeoPoint p = new GeoPoint(toPoint);
            double Length = 0.3;
            double L = Math.Sqrt(Math.Pow(fromPoint.X - toPoint.X, 2) + Math.Pow(fromPoint.Y - toPoint.Y, 2));
            if(L > Length)
            {
                p = new GeoPoint(fromPoint.X + Length * (toPoint.X - fromPoint.X) / L, fromPoint.Y + Length * (toPoint.Y - fromPoint.Y) / L);
            }

            return p; 
        }
    }
}

using System;
using System.Collections.Generic;

using SuperMap.Data;
namespace SPH.PLAS.ESA
{
    public struct SelectedFacility
    {
        public string RsltNo { get; set; }
        public FacilityType FacilityType { get; set; }
        public string Code { get; set; }
        public string KeyValue { get; set; }
        public string Label { get; set; }
        public string Name { get; set; }
        public string ConstNo { get; set; }
        public int ID { get; set; }
        public short RsltStatus { get; set; }
        public string UdbPath { get; set; }
        public Point2D Geometry { get; set; }
        public List<int> VirtualValves { get; set; }
        public int[] BarrierNodeIds { get; set; }
        public int[] BarrierEdgeIds { get; set; }
        public Recordset ResultRecordset { get; set; }
    }
    public enum FacilityType
    {
        Edge, Node
    }
}

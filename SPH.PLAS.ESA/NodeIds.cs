using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPH.PLAS.ESA
{
    public struct NodeIds
    {
        public HashSet<int> ValveNodeIds { get; set; }
        public int[] BarrierNodeIds { get; set; }
        public int[] FirstBarrierNodeIds { get; set; }
        public int[] RgltNodeIds { get; set; }
        public HashSet<int> CustomerNodeIds { get; set; }
        public NodeIds(HashSet<int> valveNodeIds, int[] firstBarrierNodeIds, int[] barrierNodeIds, int[] rgltNodeIds, HashSet<int> customerNodeIds)
        {
            ValveNodeIds = valveNodeIds;
            BarrierNodeIds = barrierNodeIds;
            FirstBarrierNodeIds = firstBarrierNodeIds;
            RgltNodeIds = rgltNodeIds;
            CustomerNodeIds = customerNodeIds;
        }
    }
}

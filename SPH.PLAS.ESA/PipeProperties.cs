using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPH.PLAS.ESA
{
    class PipeProperties
    {
        public PipeProperties(int smID, double length)
        {
            SmID = smID.ToString();
            Length = length;
        }
        public string SmID { get; set; }
        public double Length { get; set; }
    }
}

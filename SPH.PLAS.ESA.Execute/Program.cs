using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using SPH.PLAS.ESA;
namespace SPH.PLAS.ESA.Execute
{
    class Program
    {
        static void Main(string[] args)
        {
            string rsltNo = args[0];
            ExecuteESA esa = ExecuteESA.GetInstace();
            esa.ExecuteSituationAnalyst(rsltNo);

        }
    }
}

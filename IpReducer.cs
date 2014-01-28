using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Hadoop.MapReduce;

namespace CSE6410MapReduce
{
    public class IpReducer : ReducerCombinerBase
    {
        public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
        {
            context.EmitKeyValue(key, values.Count().ToString());
        }
    }
}

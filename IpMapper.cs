using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Hadoop.MapReduce;

namespace CSE6410MapReduce
{
    class IpMapper : MapperBase
    {
        public String FilterIP(string input)
        {
            Regex r = new Regex(@"^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}");
            Match ipMatch = r.Match(input);
            return ipMatch.ToString();
        }

        public override void Map(string inputLine, MapperContext context)
        {
            int one = 1;
            context.EmitKeyValue(FilterIP(inputLine), one.ToString());
        }


    }
}

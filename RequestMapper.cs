using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Hadoop.MapReduce;

namespace CSE6140MapReduce
{
    class RequestMapper : MapperBase
    {
        public String RequestFilter(string input)
        {
            Regex r = new Regex(@"(POST|GET)");
            Match ipMatch = r.Match(input);
            return ipMatch.ToString();
        }
        public override void Map(string inputLine, MapperContext context)
        {
            int one = 1;
            context.EmitKeyValue(RequestFilter(inputLine), one.ToString());
        }
    }
}

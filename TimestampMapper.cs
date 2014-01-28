using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Hadoop.MapReduce;

namespace CSE6140MapReduce
{
    class TimestampMapper : MapperBase
    {
        public String TimestampFilter(string input)
        {
            Regex r = new Regex(@"\d{2}\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2}\s\+\d{4}");
            Match timeMatch = r.Match(input);
            return timeMatch.ToString();
        }

        public override void Map(string inputLine, MapperContext context)
        {
            int one = 1;
            context.EmitKeyValue(TimestampFilter(inputLine), one.ToString());
        }
    }
}

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Hadoop.MapReduce;

namespace CSE6140MapReduce
{
    class PathMapper : MapperBase
    {
        public String PathFilter(string input)
        {
            Regex r = new Regex(@"\/var[^\s]+((?!\sGET)|(?!\sPOST))");
            Match pathMatch = r.Match(input);
            String path = pathMatch.ToString();
            if (pathMatch.Success)
            {
                path = path.Substring(1, path.Length - 1);
                return Path.GetFileName(path).ToLower();
            }
            return null;
        }
        public override void Map(string inputLine, MapperContext context)
        {
            int one = 1;
            context.EmitKeyValue(PathFilter(inputLine), one.ToString());
        }
    }
}

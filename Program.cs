/*
 * Author: Thanh Ky Quan, Zhengbo Li
 * Class: CX4140/CSE6140
 * Fall 2013
 */ 
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Hadoop.MapReduce;
using System.Text.RegularExpressions;
using System.IO;
using Microsoft.WindowsAzure.Storage.Blob;
namespace CSE6140MapReduce
{
    class Program
    {
        public class IPJob2G : HadoopJob<IpMapper, IpReducer>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                HadoopJobConfiguration config = new HadoopJobConfiguration();
                config.InputPath = "/http2g.log";
                config.OutputFolder = "/Output4/ipCount2g";
                return config;
            }
        }
        
        public class IPJob1G : HadoopJob<IpMapper, IpReducer>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                HadoopJobConfiguration config = new HadoopJobConfiguration();
                config.InputPath = "/http1g.log";
                config.OutputFolder = "/Output4/ipCount1g";
                return config;
            }
        }

        public class IPJob512M : HadoopJob<IpMapper, IpReducer>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                HadoopJobConfiguration config = new HadoopJobConfiguration();
                config.InputPath = "/http512m.log";
                config.OutputFolder = "/Output4/ipCount512m";
                return config;
            }
        }

        public class IPJob256M : HadoopJob<IpMapper, IpReducer>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                HadoopJobConfiguration config = new HadoopJobConfiguration();
                config.InputPath = "/http256m.log";
                config.OutputFolder = "/Output4/ipCount256m";
                return config;
            }
        }

        public class IPJob48M : HadoopJob<IpMapper, IpReducer>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                HadoopJobConfiguration config = new HadoopJobConfiguration();
                config.InputPath = "/http48m.log";
                config.OutputFolder = "/Output4/ipCount48m";
                return config;
            }
        }

        public class HTTPRequestJob2G : HadoopJob<RequestMapper, RequestReducer>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                HadoopJobConfiguration config = new HadoopJobConfiguration();
                config.InputPath = "/http2g.log";
                config.OutputFolder = "/Output/request2g";
                return config;
            }
        }

        public class HTTPRequestJob1G : HadoopJob<RequestMapper, RequestReducer>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                HadoopJobConfiguration config = new HadoopJobConfiguration();
                config.InputPath = "/http1g.log";
                config.OutputFolder = "/Output/request1g";
                return config;
            }
        }

        public class HTTPRequestJob512M : HadoopJob<RequestMapper, RequestReducer>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                HadoopJobConfiguration config = new HadoopJobConfiguration();
                config.InputPath = "/http512m.log";
                config.OutputFolder = "/Output/request512m";
                return config;
            }
        }

        public class HTTPRequestJob256M : HadoopJob<RequestMapper, RequestReducer>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                HadoopJobConfiguration config = new HadoopJobConfiguration();
                config.InputPath = "/http256m.log";
                config.OutputFolder = "/Output/request256m";
                return config;
            }
        }

        public class HTTPRequestJob48M : HadoopJob<RequestMapper, RequestReducer>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                HadoopJobConfiguration config = new HadoopJobConfiguration();
                config.InputPath = "/http48m.log";
                config.OutputFolder = "/Output/request48m";
                return config;
            }
        }

        public class TimeFilterJob2G : HadoopJob<TimestampMapper, TimestampReducer>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                HadoopJobConfiguration config = new HadoopJobConfiguration();
                config.InputPath = "/http2g.log";
                config.OutputFolder = "/Output/timefilter2g";
                return config;
            }
        }

        public class TimeFilterJob1G : HadoopJob<TimestampMapper, TimestampReducer>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                HadoopJobConfiguration config = new HadoopJobConfiguration();
                config.InputPath = "/http1g.log";
                config.OutputFolder = "/Output/timefilter1g";
                return config;
            }
        }

        public class TimeFilterJob512M : HadoopJob<TimestampMapper, TimestampReducer>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                HadoopJobConfiguration config = new HadoopJobConfiguration();
                config.InputPath = "/http512m.log";
                config.OutputFolder = "/Output/timefilter512m";
                return config;
            }
        }

        public class TimeFilterJob256M : HadoopJob<TimestampMapper, TimestampReducer>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                HadoopJobConfiguration config = new HadoopJobConfiguration();
                config.InputPath = "/http256m.log";
                config.OutputFolder = "/Output/timefilter256m";
                return config;
            }
        }

        public class TimeFilterJob48M : HadoopJob<TimestampMapper, TimestampReducer>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                HadoopJobConfiguration config = new HadoopJobConfiguration();
                config.InputPath = "/http48m.log";
                config.OutputFolder = "/Output/timefilter48m";
                return config;
            }
        }

        public class PathFilterJob48M : HadoopJob<PathMapper, PathReducer>
        {
            public override HadoopJobConfiguration Configure(ExecutorContext context)
            {
                HadoopJobConfiguration config = new HadoopJobConfiguration();
                config.InputPath = "/http48m.log";
                config.OutputFolder = "/Output/pathfilter48m";
                return config;
            }
        }

        static void Main(string[] args)
        {
            
            Environment.SetEnvironmentVariable("HADOOP_HOME", @"c:\hadoop");
            Environment.SetEnvironmentVariable("Java_HOME", @"c:\hadoop\jvm");

            // Cluster url
            var clusterName = new Uri("");

            // Cluster username
            const String userName = "admin";

            // Azure Hadoop Username
            const String hadoopUser = "";

            // Azure Hadoop password
            const String password = "";

            // Azure Hadoop storage account
            // usually username.blob.core.windows.net
            const String storageAccount = "";

            // Azure storage key
            const String storageKey = "";

            // Container
            const String container = "";
            var hadoop = Hadoop.Connect(clusterName, userName, hadoopUser, password, storageAccount, storageKey, container, true);

            #region IP Filter 
            hadoop.MapReduceJob.ExecuteJob<IPJob2G>();
            hadoop.MapReduceJob.ExecuteJob<IPJob1G>();
            hadoop.MapReduceJob.ExecuteJob<IPJob512M>();
            hadoop.MapReduceJob.ExecuteJob<IPJob256M>();
            hadoop.MapReduceJob.ExecuteJob<IPJob48M>();
            #endregion IP Filter

            #region Request filter
            hadoop.MapReduceJob.ExecuteJob<HTTPRequestJob2G>();
            hadoop.MapReduceJob.ExecuteJob<HTTPRequestJob1G>();
            hadoop.MapReduceJob.ExecuteJob<HTTPRequestJob512M>();
            hadoop.MapReduceJob.ExecuteJob<HTTPRequestJob256M>();
            hadoop.MapReduceJob.ExecuteJob<HTTPRequestJob48M>();
            #endregion Request filter
            #region Timestamp filter
            hadoop.MapReduceJob.ExecuteJob<TimeFilterJob2G>();
            hadoop.MapReduceJob.ExecuteJob<TimeFilterJob1G>();
            hadoop.MapReduceJob.ExecuteJob<TimeFilterJob512M>();
            hadoop.MapReduceJob.ExecuteJob<TimeFilterJob256M>();
            hadoop.MapReduceJob.ExecuteJob<TimeFilterJob48M>();
            #endregion Timestamp filter
            #region Path filter
            hadoop.MapReduceJob.ExecuteJob<PathFilterJob48M>();
            #endregion Path filter
        }
    }
}

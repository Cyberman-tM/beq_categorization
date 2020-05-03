using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Web;
using System.Xml.Serialization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace tlhingan.beq
{


    public class bulkCatData
    {
        public string n;
        public string l;
        public string d;
    }

    public class bulkW2CData
    {
        public string k;
        public string n;
    }

    public class bulkWordData
    {
        public string n;
    }

    public class mainCatData : TableEntity
    {
        public int mainCatCount { get; set; } = 0;

        public mainCatData()
        {
            PartitionKey = "mainCatData";
            RowKey = "count";
        }

        public mainCatData(string partKey = "mainCatData", string rowKey = "count")
        {
            PartitionKey = partKey;
            RowKey = rowKey;
        }
    };

    public class mainWordData : TableEntity
    {
        public int wordCount { get; set; } = 0;

        public mainWordData()
        {
            PartitionKey = "mainWordData";
            RowKey = "count";
        }

        public mainWordData(string partKey = "mainWordData", string rowKey = "count")
        {
            PartitionKey = partKey;
            RowKey = rowKey;
        }
    }

    public static partial class beq_categorization
    {
        private static int lastBulkCat = 0;

        /// <summary>
        ///     Wake up
        /// </summary>
        [FunctionName("wakeup")]
        public static async Task<IActionResult> wakeup(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            //Offenbar bekommt die Funktion automatisch ein korrekt eingerichtetes Objekt für die Tabelle mitgegeben
            [Table("categorization")] CloudTable tabCats,
            ILogger log)
        {
            //Nothing to do...

            return null;
        }

        public static async Task<int> getNextCatNumber(CloudTable tabCats)
        {
            mainCatData MCD = new mainCatData();

            TableOperation query = TableOperation.Retrieve<mainCatData>("mainCatData", "count");
            TableResult tabRes = await tabCats.ExecuteAsync(query);
            if (tabRes.Result != null)
                MCD = (mainCatData)tabRes.Result;

            MCD.mainCatCount++;

            TableOperation writeBack = TableOperation.InsertOrReplace(MCD);
            await tabCats.ExecuteAsync(writeBack);

            return MCD.mainCatCount;
        }

        public static async Task<int> getNextWordNumber(CloudTable tabCats)
        {
            mainWordData MWD = new mainWordData();

            TableOperation query = TableOperation.Retrieve<mainWordData>("mainWordData", "count");
            TableResult tabRes = await tabCats.ExecuteAsync(query);
            if (tabRes.Result != null)
                MWD = (mainWordData)tabRes.Result;

            MWD.wordCount++;

            TableOperation writeBack = TableOperation.InsertOrReplace(MWD);
            await tabCats.ExecuteAsync(writeBack);

            return MWD.wordCount;
        }
    }
}
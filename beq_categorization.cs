using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
    public class beqDef
    {
        /// <summary>
        /// Category, ID to Name
        /// </summary>
        public const string partCatI2N = "CatI2N";

        /// <summary>
        /// Category, Name to ID
        /// </summary>
        public const string partCatN2I = "CatN2I";

        /// <summary>
        /// Category, Category to Words(IDs)
        /// </summary>
        public const string partCat2Words = "Cat2Words";

        /// <summary>
        /// Category, Descriptions
        /// </summary>
        public const string partCatDesc = "CatDesc";

        //
        //
        //

        /// <summary>
        /// Words, ID to Name
        /// </summary>
        public const string partWordI2N = "WordI2N";

        /// <summary>
        /// Words, Name to ID
        /// </summary>
        public const string partWordN2I = "WordN2I";

        /// <summary>
        /// Words, Word to Categories
        /// </summary>
        public const string partWord2Cat = "Word2Cat";
    }


    /// <summary>
    /// Secondary index for Categories - get ID from name first if necessary,
    /// Name class also contains number of subclasses
    /// </summary>
    public class CatI2N : TableEntity
    {
        public string fullName { get; set; } = "";
        public CatI2N()
        {
            PartitionKey = beqDef.partCatI2N;
            RowKey = "";
        }

        /// <summary>
        /// ROWKEY DARF NICHT LEER SEIN!
        /// </summary>
        /// <param name="partKey">fixer Wert, ignore</param>
        /// <param name="rowKey">ID der Kategorie, KID</param>
        public CatI2N(string partKey = beqDef.partCatI2N, string i_rowKey = "")
        {
            PartitionKey = partKey;
            RowKey = i_rowKey;
        }

        public void setKID(string i_KID)
        {
            RowKey = i_KID;
        }

        public void setName(string i_fullName)
        {
            fullName = i_fullName;
        }
    }

    public class CatN2I : TableEntity
    {
        public string KID { get; set; } = "";

        /// <summary>
        /// Number of subcategories, if this is a supercategory
        /// </summary>
        public int subCatCount { get; set; } = 0;

        public CatN2I()
        {
            PartitionKey = beqDef.partCatN2I;
            RowKey = "";
        }

        public CatN2I(string partKey = beqDef.partCatN2I, string i_rowKey = "")
        {
            PartitionKey = partKey;
            RowKey = i_rowKey;
        }

        public void setKID(string i_KID)
        {
            KID = i_KID;
        }

        public void setName(string i_Name)
        {
            RowKey = i_Name;
        }
    }

    public class CatI2Desc : TableEntity
    {
        public string Langu { get; set; } = "";
        public string Desc { set; get; } = "";

        public CatI2Desc()
        {
            PartitionKey = beqDef.partCatDesc;
            RowKey = "";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="partKey">fixed value, ignore</param>
        /// <param name="rowKey">KID</param>
        public CatI2Desc(string partKey = beqDef.partCatDesc, string i_rowKey = "")
        {
            PartitionKey = partKey;
            RowKey = i_rowKey;
        }

        public void setKID(string i_KID)
        {
            RowKey = i_KID;
        }

        public void setDesc(string i_langu, string i_desc)
        {
            Langu = i_langu;
            Desc = i_desc;
        }
    }

    /*
        /// <summary>
        /// Category Descriptions
        /// Key: KID
        /// Subkey: Langu
        /// </summary>
        public class catDescs : TableEntity
        {
            /// <summary>
            /// KID <> ( Langu <> Description)
            /// </summary>
            private Dictionary<string, Dictionary<string, string>> catDesc;

            /// <summary>
            /// JSonDesc
            /// </summary>
            public string JD { set; get; } = "";

            public catDescs()
            {
                this.PartitionKey = beqDef.c_partCat;
                this.RowKey = beqDef.c_rowCatDesc;

                reload();
            }

            public catDescs(string partition = beqDef.c_partCat, string rowkey = beqDef.c_rowCatDesc)
            {
                this.PartitionKey = beqDef.c_partCat;
                this.RowKey = beqDef.c_rowCatDesc;

                reload();
            }

            public void reload()
            {
                catDesc = JsonConvert.DeserializeObject<Dictionary<string, Dictionary<string, string>>>(JD);

                if (catDesc == null)
                    catDesc = new Dictionary<string, Dictionary<string, string>>();
            }

            public void addDesc(string KID, string Langu, string Desc)
            {
                Dictionary<string, string> aDesc = new Dictionary<string, string>();
                if (catDesc.ContainsKey(KID))
                    aDesc = catDesc[KID];
                else
                    catDesc.Add(KID, new Dictionary<string, string>());

                aDesc[Langu] = Desc;

                catDesc[KID] = aDesc;

                JD = JsonConvert.SerializeObject(catDesc);
            }
        }

        /// <summary>
        /// Category IDs
        /// Key: full name
        /// (simple dictionary)
        /// </summary>
        public class catIDs : TableEntity
        {
            public Dictionary<string, string> name2ID;

            public string JN2I { get; set; } = "";

            public catIDs()
            {
                this.PartitionKey = beqDef.c_partCat;
                this.RowKey = beqDef.c_rowCatKid;

                reload();
            }

            public catIDs(string partition = beqDef.c_partCat, string rowkey = beqDef.c_rowCatKid)
            {
                this.PartitionKey = beqDef.c_partCat;
                this.RowKey = beqDef.c_rowCatKid;

                reload();
            }

            public void reload()
            {
                name2ID = JsonConvert.DeserializeObject<Dictionary<string, string>>(JN2I);
                if (name2ID == null)
                    name2ID = new Dictionary<string, string>();
            }

            public void addN2I(string name, string KID)
            {
                if (!name2ID.ContainsKey(name))
                    name2ID.Add(name, KID);

                JN2I = JsonConvert.SerializeObject(name2ID);
            }

            public string getID(string name)
            {
                string tmpRet = null;
                if (name2ID.ContainsKey(name))
                    tmpRet = name2ID[name];

                return tmpRet;
            }
        }

        /// <summary>
        /// WIDs pro KID
        /// </summary>
        public class catWIDS : TableEntity
        {
            /// <summary>
            /// WIDs per KID
            /// </summary>
            public Dictionary<string, List<string>> catWIDs;
            public string JSonKWID { set; get; } = "";

            public catWIDS()
            {
                this.PartitionKey = beqDef.c_partCat;
                this.RowKey = beqDef.c_rowCatWID;

                reload();
            }
            public catWIDS(string partition = beqDef.c_partCat, string rowkey = beqDef.c_rowCatWID)
            {
                this.PartitionKey = beqDef.c_partCat;
                this.RowKey = beqDef.c_rowCatWID;

                reload();
            }

            public void reload()
            {
                catWIDs = JsonConvert.DeserializeObject<Dictionary<string, List<string>>>(JSonKWID);
                if (catWIDs == null)
                    catWIDs = new Dictionary<string, List<string>>();
            }

            public void addWID(string KID, string WID)
            {
                List<string> aKID = new List<string>();
                if (!catWIDs.ContainsKey(KID))
                    catWIDs.Add(KID, aKID);
                else
                    aKID = catWIDs[KID];

                if (!aKID.Contains(WID))
                {
                    aKID.Add(WID);
                    catWIDs[KID] = aKID;
                    JSonKWID = JsonConvert.SerializeObject(catWIDs);
                }
            }
        }

        /// <summary>
        /// Einfach nur ein Behälter
        /// </summary>
        public class fullCat
        {
            public catDescs allDescs;
            public catIDs allIDs;
            public catWIDS allWIDs;
        }


        /// <summary>
        /// Jetzt kommen die Worte :-)
        /// </summary>

        /// <summary>
        /// word IDs
        /// Key: full word
        /// (simple dictionary)
        /// </summary>
        public class wordIDs : TableEntity
        {
            public Dictionary<string, string> name2ID;

            public string JN2I { get; set; } = "";

            public wordIDs()
            {
                this.PartitionKey = beqDef.c_partWord;
                this.RowKey = beqDef.c_rowWordWID;

                reload();
            }

            public wordIDs(string partition = beqDef.c_partWord, string rowkey = beqDef.c_rowWordWID)
            {
                this.PartitionKey = beqDef.c_partWord;
                this.RowKey = beqDef.c_rowWordWID;

                reload();
            }

            public void reload()
            {
                name2ID = JsonConvert.DeserializeObject<Dictionary<string, string>>(JN2I);
                if (name2ID == null)
                    name2ID = new Dictionary<string, string>();
            }

            public void addN2I(string name, string WID)
            {
                if (!name2ID.ContainsKey(name))
                    name2ID.Add(name, WID);

                JN2I = JsonConvert.SerializeObject(name2ID);
            }

            public string getID(string name)
            {
                string tmpRet = null;
                if (name2ID.ContainsKey(name))
                    tmpRet = name2ID[name];

                return tmpRet;
            }
        }

        /// <summary>
        /// KIDs pro WID
        /// </summary>
        public class wordKIDS : TableEntity
        {
            /// <summary>
            /// KIDs per WID
            /// </summary>
            public Dictionary<string, List<string>> wordKIDs;
            public string JSonWKID { set; get; } = "";

            public wordKIDS()
            {
                this.PartitionKey = beqDef.c_partWord;
                this.RowKey = beqDef.c_rowWordKID;

                reload();
            }
            public wordKIDS(string partition = beqDef.c_partWord, string rowkey = beqDef.c_rowWordKID)
            {
                this.PartitionKey = beqDef.c_partWord;
                this.RowKey = beqDef.c_rowWordKID;

                reload();
            }

            public void reload()
            {
                wordKIDs = JsonConvert.DeserializeObject<Dictionary<string, List<string>>>(JSonWKID);
                if (wordKIDs == null)
                    wordKIDs = new Dictionary<string, List<string>>();
            }

            public void addKID(string WID, string KID)
            {
                List<string> aKID = new List<string>();
                if (!wordKIDs.ContainsKey(WID))
                    wordKIDs.Add(WID, aKID);
                else
                    aKID = wordKIDs[WID];

                aKID.Add(KID);

                wordKIDs[WID] = aKID;

                JSonWKID = JsonConvert.SerializeObject(wordKIDs);
            }
        }

        public class fullWord
        {
            public wordKIDS allKIDS = new wordKIDS();
            public wordIDs allIDs = new wordIDs();
        }
    */

    /// <summary>
    /// Secondary index for words - get ID from name first if necessary,
    /// Name class also contains number of subclasses
    /// </summary>
    public class WordI2N : TableEntity
    {
        public string fullName { get; set; } = "";
        public WordI2N()
        {
            PartitionKey = beqDef.partWordI2N;
            RowKey = "";
        }

        /// <summary>
        /// ROWKEY DARF NICHT LEER SEIN!
        /// </summary>
        /// <param name="partKey">fixer Wert, ignore</param>
        /// <param name="rowKey">ID des Worts, WID</param>
        public WordI2N(string partKey = beqDef.partWordI2N, string i_rowKey = "")
        {
            PartitionKey = partKey;
            RowKey = i_rowKey;
        }

        public void setWID(string i_WID)
        {
            RowKey = i_WID;
        }

        public void setName(string i_fullName)
        {
            fullName = i_fullName;
        }
    }

    public class WordN2I : TableEntity
    {
        public string WID { get; set; } = "";

        public WordN2I()
        {
            PartitionKey = beqDef.partWordN2I;
            RowKey = "";
        }

        public WordN2I(string partKey = beqDef.partWordN2I, string i_rowKey = "")
        {
            PartitionKey = partKey;
            RowKey = i_rowKey;
        }

        public void setWID(string i_WID)
        {
            WID = i_WID;
        }

        public void setName(string i_Name)
        {
            RowKey = i_Name;
        }
    }

    public class Word2Cat : TableEntity
    {
        public string intCatList { set; get; } = "";

        public List<string> catList;
        public Word2Cat()
        {
            PartitionKey = beqDef.partWord2Cat;
            RowKey = "";
            reload();
        }
        public Word2Cat(string partKey = beqDef.partWord2Cat, string rowKey = "")
        {
            PartitionKey = partKey;
            RowKey = rowKey;

            reload();
        }

        public void reload()
        {
            catList = JsonConvert.DeserializeObject<List<string>>(intCatList);
            catList = catList ?? new List<string>();
        }

        public void setWord(string WID)
        {
            RowKey = WID;
        }

        public void addCat(string KID)
        {
            catList.Add(KID);
            intCatList = JsonConvert.SerializeObject(catList);
        }
    }

    public class Cat2Word : TableEntity
    {
        public string intWordList { set; get; } = "";

        public List<string> wordList;
        public Cat2Word()
        {
            PartitionKey = beqDef.partCat2Words;
            RowKey = "";
            reload();
        }
        public Cat2Word(string partKey = beqDef.partCat2Words, string rowKey = "")
        {
            PartitionKey = partKey;
            RowKey = rowKey;

            reload();
        }

        public void reload()
        {
            wordList = JsonConvert.DeserializeObject<List<string>>(intWordList);
            wordList = wordList ?? new List<string>();
        }

        public void setCat(string KID)
        {
            RowKey = KID;
        }

        public void addWord(string WID)
        {
            wordList.Add(WID);
            intWordList = JsonConvert.SerializeObject(wordList);
        }
    }

    public class tmpData : TableEntity
    {

        public string daten { get; set; } = "";

        public tmpData()
        {
            PartitionKey = "tmp";
            RowKey = "data";

        }

        public tmpData(string partKey = "tmp", string rowkey = "data")
        {
            PartitionKey = partKey;
            RowKey = rowkey;
        }
    }
    public static partial class beq_categorization
    {
        public static TableBatchOperation bulkTO;
        public static TableBatchOperation bulkTO2;
        public static TableBatchOperation bulkTO3;
        private static int lastBulkCat = 0;
        private static int lastBulkWord = 0;

        private static List<string> tmpAllW2NI;

        [FunctionName("catWord")]
        public static async Task<IActionResult> catWord(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            //Offenbar bekommt die Funktion automatisch ein korrekt eingerichtetes Objekt für die Tabelle mitgegeben
            [Table("categorization")] CloudTable tabCats,
            ILogger log)
        {
            string i_WID = req.Query["WID"];
            string i_KID = req.Query["KID"];

            string i_word = req.Query["fullWord"];
            string i_cat = req.Query["catName"];

            i_word = i_word ?? "";
            i_cat = i_cat ?? "";

            i_WID = i_WID ?? "";
            i_KID = i_KID ?? "";

            await intCatWord(tabCats, i_WID, i_KID, i_word, i_cat);

            return new OkObjectResult("");
        }

        [FunctionName("catWordBulk")]
        public static async Task<IActionResult> catWordBulk(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            //Offenbar bekommt die Funktion automatisch ein korrekt eingerichtetes Objekt für die Tabelle mitgegeben
            [Table("categorization")] CloudTable tabCats,
            ILogger log)
        {
            List<bulkW2CData> allC2W;

            //Bulk data - array of objects with name, langu, desc as attributes
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            allC2W = JsonConvert.DeserializeObject<List<bulkW2CData>>(requestBody);
            string dummy;

            int bCount = 0;
            foreach (bulkW2CData oneC2W in allC2W)
            {
                bCount++;
                await intCatWord(tabCats, "", "", oneC2W.n, oneC2W.k, true);
                if (bCount == 99)
                {
                    dummy = tabCats.ExecuteBatchAsync(bulkTO).Result.ToString(); ;
                    dummy = tabCats.ExecuteBatchAsync(bulkTO2).Result.ToString(); ;
                    bCount = 0;
                    bulkTO.Clear();
                    bulkTO2.Clear();
                }
            }

            dummy = tabCats.ExecuteBatchAsync(bulkTO).Result.ToString(); ;
            dummy = tabCats.ExecuteBatchAsync(bulkTO2).Result.ToString(); ;
            return new OkObjectResult("");
        }

        public static async Task<IActionResult> intCatWord(CloudTable tabCats, string i_WID, string i_KID, string i_word, string i_cat, bool bulk = false)
        {
            //ID anhand vollständigem Bezeichner nachlesen
            if (i_WID == "")
            {
                TableOperation query = TableOperation.Retrieve<WordN2I>(beqDef.partWordN2I, i_word);
                TableResult tabRes = await tabCats.ExecuteAsync(query);
                WordN2I tmpWN2I = (WordN2I)tabRes.Result;
                tmpWN2I = tmpWN2I ?? new WordN2I();
                i_WID = tmpWN2I.WID;
            }

            if (i_KID == "")
            {
                TableOperation query = TableOperation.Retrieve<CatN2I>(beqDef.partCatN2I, i_cat);
                TableResult tabRes = await tabCats.ExecuteAsync(query);
                CatN2I tmpCN2I = (CatN2I)tabRes.Result;
                tmpCN2I = tmpCN2I ?? new CatN2I();
                i_KID = tmpCN2I.KID;
            }

            if (i_WID != "" && i_KID != "")
            {
                TableOperation query = TableOperation.Retrieve<Word2Cat>(beqDef.partWord2Cat, i_WID);
                TableResult tabRes = await tabCats.ExecuteAsync(query);
                Word2Cat newW2C = (Word2Cat)tabRes.Result;
                newW2C = newW2C ?? new Word2Cat();
                newW2C.reload();

                newW2C.setWord(i_WID);
                newW2C.addCat(i_KID);

                Task<TableResult> dummyReturn;
                if (!bulk)
                    dummyReturn = tabCats.ExecuteAsync(TableOperation.InsertOrReplace(newW2C));
                else
                    bulkTO.Add(TableOperation.InsertOrReplace(newW2C));

                query = TableOperation.Retrieve<Word2Cat>(beqDef.partCat2Words, i_KID);
                tabRes = await tabCats.ExecuteAsync(query);
                Cat2Word newC2W = (Cat2Word)tabRes.Result;
                newC2W = newC2W ?? new Cat2Word();
                newC2W.reload();

                newC2W.setCat(i_KID);
                newC2W.addWord(i_WID);

                if (!bulk)
                    await tabCats.ExecuteAsync(TableOperation.InsertOrReplace(newC2W));
                else
                    bulkTO2.Add(TableOperation.InsertOrReplace(newC2W));
            }
            return new OkObjectResult("");
        }

        [FunctionName("createWord")]
        public static async Task<IActionResult> createWord(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            //Offenbar bekommt die Funktion automatisch ein korrekt eingerichtetes Objekt für die Tabelle mitgegeben
            [Table("categorization")] CloudTable tabCats,
            ILogger log)
        {
            string tmpWID = "";
            string i_wordName = req.Query["fullWord"];
            i_wordName = i_wordName ?? "";

            if (i_wordName != "")
            {
                tmpWID = intCreateWord(tabCats, i_wordName).Result.ToString();
            }

            return new OkObjectResult(tmpWID);
        }

        [FunctionName("createWordBulk")]
        public static async Task<IActionResult> createWordBulk(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
        //Offenbar bekommt die Funktion automatisch ein korrekt eingerichtetes Objekt für die Tabelle mitgegeben
        [Table("categorization")] CloudTable tabCats,
        ILogger log)
        {
            List<bulkWordData> allBW;

            //Bulk data - array of objects with name, langu, desc as attributes
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            if (requestBody == null || requestBody == "")
                return null;

            allBW = JsonConvert.DeserializeObject<List<bulkWordData>>(requestBody);

            mainWordData MWD = new mainWordData();
            TableOperation query = TableOperation.Retrieve<mainWordData>("mainWordData", "count");
            TableResult tabRes = await tabCats.ExecuteAsync(query);
            if (tabRes.Result != null)
                MWD = (mainWordData)tabRes.Result;

            MWD.wordCount++;
            lastBulkWord = MWD.wordCount;

            bulkTO = new TableBatchOperation();
            bulkTO2 = new TableBatchOperation();

            tmpAllW2NI = await intGetAllWordNames(tabCats);

            int bCount = 0;
            int xcount = 0;
            List<Task<IList<TableResult>>> batchTasks = new List<Task<IList<TableResult>>>();
            foreach (bulkWordData oneWord in allBW)
            {
                xcount++;
                if (oneWord.n == null || oneWord.n == "" || bCount == 54)
                {
                    var x = 0;
                }
                bCount++;
                await intCreateWord(tabCats, oneWord.n, true);
                if (bCount == 99)
                {
                    if (bulkTO.Count > 0)
                        batchTasks.Add(tabCats.ExecuteBatchAsync(bulkTO));
                    if (bulkTO2.Count > 0)
                        batchTasks.Add(tabCats.ExecuteBatchAsync(bulkTO2));
                    bCount = 0;
                    bulkTO.Clear();
                    bulkTO2.Clear();
                }
            }

            MWD.wordCount = lastBulkWord;
            TableOperation writeBack = TableOperation.InsertOrReplace(MWD);
            var dummy = tabCats.ExecuteAsync(writeBack);

            if (bulkTO.Count > 0)
                batchTasks.Add(tabCats.ExecuteBatchAsync(bulkTO));
            if (bulkTO2.Count > 0)
                batchTasks.Add(tabCats.ExecuteBatchAsync(bulkTO2));

            Task.WaitAll(batchTasks.ToArray());

            return new OkObjectResult("");
        }

        public static async Task<List<string>> intGetAllWordNames(CloudTable tabCats)
        {
            //Prefetch all known words
            List<WordN2I> _tmpAllW2NI = new List<WordN2I>();
            TableContinuationToken tok = new TableContinuationToken();
            TableQuery<WordN2I> newQuery = new TableQuery<WordN2I>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, beqDef.partWordN2I));
            newQuery.TakeCount = 9999;
            do
            {
                TableQuerySegment<WordN2I> segment = await tabCats.ExecuteQuerySegmentedAsync(newQuery, tok);
                tok = segment.ContinuationToken;
                _tmpAllW2NI.AddRange(segment.Results);
            }
            while (tok != null);
            tmpAllW2NI = new List<string>();
            tmpAllW2NI.AddRange(_tmpAllW2NI.Select(x => x.RowKey).ToArray());

            return tmpAllW2NI;
        }

        public static async Task<IActionResult> intCreateWord(CloudTable tabCats, string i_wordName, bool bulk = false)
        {
            string tmpWID = "";
            //Do we know this word already?
            TableResult tabRes = new TableResult();
            tabRes.Result = null;
            int wordIndex = -1;
            if (!bulk)
            {
                TableOperation query = TableOperation.Retrieve<WordN2I>(beqDef.partWordN2I, i_wordName);
                tabRes = await tabCats.ExecuteAsync(query);
            }
            else
            {
                wordIndex = tmpAllW2NI.IndexOf(i_wordName);
            }
            if (tabRes.Result == null && wordIndex == -1)
            {
                if (!bulk)
                    tmpWID = 'W' + getNextWordNumber(tabCats).Result.ToString("d6");
                else
                    tmpWID = 'W' + (++lastBulkWord).ToString("d6");

                WordN2I newWordN2I = new WordN2I();
                newWordN2I.setName(i_wordName);
                newWordN2I.setWID(tmpWID);

                WordI2N newWordI2N = new WordI2N();
                newWordI2N.setName(i_wordName);
                newWordI2N.setWID(tmpWID);

                if (!bulk)
                {
                    var dummyReturn = tabCats.ExecuteAsync(TableOperation.InsertOrReplace(newWordN2I));
                    await tabCats.ExecuteAsync(TableOperation.InsertOrReplace(newWordI2N));
                }
                else
                {
                    bulkTO.Add(TableOperation.InsertOrReplace(newWordN2I));
                    bulkTO2.Add(TableOperation.InsertOrReplace(newWordI2N));
                }

            }
            return new OkObjectResult("");
        }


        [FunctionName("createCategBulk")]
        public static async Task<IActionResult> createCategBulk(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            //Offenbar bekommt die Funktion automatisch ein korrekt eingerichtetes Objekt für die Tabelle mitgegeben
            [Table("categorization")] CloudTable tabCats,
            ILogger log)
        {
            List<bulkCatData> allBCD;

            //Bulk data - array of objects with name, langu, desc as attributes
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            allBCD = JsonConvert.DeserializeObject<List<bulkCatData>>(requestBody);

            mainCatData MCD = new mainCatData();

            TableOperation query = TableOperation.Retrieve<mainCatData>("mainCatData", "count");
            TableResult tabRes = await tabCats.ExecuteAsync(query);
            if (tabRes.Result != null)
                MCD = (mainCatData)tabRes.Result;

            lastBulkCat = MCD.mainCatCount;
            bulkTO = new TableBatchOperation();

            foreach (bulkCatData oneBCD in allBCD)
                await intCreateCateg(tabCats, oneBCD.n, oneBCD.l, oneBCD.d, true);

            MCD.mainCatCount = lastBulkCat;
            TableOperation writeBack = TableOperation.InsertOrReplace(MCD);
            var dumRet = tabCats.ExecuteAsync(writeBack);
            if (bulkTO.Count > 0)
                await tabCats.ExecuteBatchAsync(bulkTO);
            bulkTO = null;

            return new OkObjectResult("");
        }

        [FunctionName("createCateg")]
        public static async Task<IActionResult> createCateg(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            //Offenbar bekommt die Funktion automatisch ein korrekt eingerichtetes Objekt für die Tabelle mitgegeben
            [Table("categorization")] CloudTable tabCats,
            ILogger log)
        {
            string newKID = "";
            string i_catName = req.Query["catName"];
            string i_catDesc = HttpUtility.UrlDecode(req.Query["catDesc"]);
            string i_catDLan = req.Query["catDLan"];

            i_catName = i_catName ?? "";

            if (i_catName != "")
                newKID = intCreateCateg(tabCats, i_catName, i_catDLan, i_catDesc, false).Result.ToString();

            return new OkObjectResult(newKID);
        }

        public static async Task<IActionResult> intCreateCateg(CloudTable tabCats, string i_catName, string i_catDLan, string i_catDesc, bool bulk)
        {
            string newKID = "";
            //Do we know this category already?
            TableOperation query = TableOperation.Retrieve<CatN2I>(beqDef.partCatN2I, i_catName);
            TableResult tabRes = await tabCats.ExecuteAsync(query);
            if (tabRes.Result == null)
            {
                newKID = "";
                //Wir kennen die kategorie noch nicht, prüfen ob es eine Unterkategorie ist,
                //dann müssen wir die ID der Oberkategorie finden
                if (i_catName.Contains('_'))
                {
                    string[] subCats = i_catName.Split('_');
                    string supCat = string.Join('_', subCats, 0, subCats.Length - 1);

                    //Gibt es die Oberkategorie schon?
                    query = TableOperation.Retrieve<CatN2I>(beqDef.partCatN2I, supCat);
                    tabRes = await tabCats.ExecuteAsync(query);
                    //Superkategorie vorhanden
                    if (tabRes.Result != null)
                    {
                        //Supercategory exists, increase subcategory counter and use it for new category
                        CatN2I superCat = (CatN2I)tabRes.Result;
                        superCat.subCatCount++;
                        newKID = superCat.KID + "." + superCat.subCatCount.ToString("d3");

                        //Write back supercat, no need to wait
                        TableOperation writeBack = TableOperation.InsertOrReplace(superCat);
                        var dummyReturn = tabCats.ExecuteAsync(writeBack);
                    }
                    else
                    {
                        //Oberkategorie existiert noch nicht -> Fehler
                    }
                }
                else
                {
                    //Keine Unterkategorie, komplett neu
                    if (!bulk)
                        newKID = "K" + getNextCatNumber(tabCats).Result.ToString("d3");
                    else
                        newKID = "K" + (++lastBulkCat).ToString("d3");
                }

                //Wir haben eine ID, Kategorie kann erstellt werden
                if (newKID != "")
                {
                    CatN2I newCatN2I = new CatN2I();
                    newCatN2I.setKID(newKID);
                    newCatN2I.setName(i_catName);

                    CatI2N newCatI2N = new CatI2N();
                    newCatI2N.setKID(newKID);
                    newCatI2N.setName(i_catName);

                    CatI2Desc newCatDesc = new CatI2Desc();
                    newCatDesc.setKID(newKID);
                    newCatDesc.setDesc(i_catDLan, i_catDesc);


                    var dummyReturn = tabCats.ExecuteAsync(TableOperation.InsertOrReplace(newCatN2I));
                    await tabCats.ExecuteAsync(TableOperation.InsertOrReplace(newCatI2N));

                    //Kategoriebeschriftungen können auch nachträglich gespeichert werden, die können wir sammeln
                    if (!bulk)
                        await tabCats.ExecuteAsync(TableOperation.InsertOrReplace(newCatDesc));
                    else
                        bulkTO.Add(TableOperation.InsertOrReplace(newCatDesc));

                }
            }
            return new OkObjectResult(newKID);
        }

    }

}

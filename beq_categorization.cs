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
    public class beqDef
    {
        /// <summary>
        /// Partition Categories
        /// </summary>
        public const string c_partCat = "categ";
        /// <summary>
        /// Category IDs
        /// </summary>
        public const string c_rowCatKid = "catKID";

        /// <summary>
        /// Category Descriptions
        /// </summary>
        public const string c_rowCatDesc = "catDesc";

        /// <summary>
        /// Category WIDs
        /// </summary>
        public const string c_rowCatWID = "catWID";

        public const string c_partWord = "word";
        public const string c_rowWordWID = "wordWID";
        public const string c_rowWordKID = "wordKID";

    }


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



    public static class beq_categorization
    {
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

        /// <summary>
        /// Add a new(?) category to the list
        /// If it exists - return ID,
        /// if not - create and return ID
        /// check if it's a child (i.e. MainKateg_SubKateg_SubSubKateg and create ID accordingly)
        /// </summary>
        /// <returns></returns>
        [FunctionName("createCateg")]
        public static async Task<IActionResult> createCateg(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            //Offenbar bekommt die Funktion automatisch ein korrekt eingerichtetes Objekt für die Tabelle mitgegeben
            [Table("categorization")] CloudTable tabCats,
            ILogger log)
        {
            string tmpRet = "";
            //Prepare to read request body
            string i_catName = req.Query["catName"];
            string i_catDesc = HttpUtility.UrlDecode(req.Query["catDesc"]);
            string i_catDLan = req.Query["catDLan"];

            if (i_catName != null && i_catName != "")
            {
                if (i_catDesc == null)
                    i_catDesc = "";
                if (i_catDLan == null)
                    i_catDLan = "en";

                //Get category data from table
                fullCat allCat = getCatsData(tabCats).Result;

                tmpRet = intAddCat(allCat, i_catName, i_catDLan, i_catDesc);
                if (tmpRet != "")
                    await saveCats(tabCats, allCat);
            }

            //Entweder enthält tmpRet eine ID oder ist leer
            return new OkObjectResult(tmpRet);
        }

        public class bulkCatData
        {
            public string name;
            public string langu;
            public string desc;
        }

        /// <summary>
        /// Add many categories to the list
        /// check if it's a child (i.e. MainKateg_SubKateg_SubSubKateg and create ID accordingly)
        /// </summary>
        [FunctionName("createCategBulk")]
        public static async Task<IActionResult> createCategBulk(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            //Offenbar bekommt die Funktion automatisch ein korrekt eingerichtetes Objekt für die Tabelle mitgegeben
            [Table("categorization")] CloudTable tabCats,
            ILogger log)
        {
            string tmpRet = "";

            List<bulkCatData> allBDC;

            //Bulk data - array of objects with name, langu, desc as attributes
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            allBDC = JsonConvert.DeserializeObject<List<bulkCatData>>(requestBody);

            string i_catName = "";
            string i_catDesc = "";
            string i_catDLan = "";

            //Get category data from table
            fullCat allCat = getCatsData(tabCats).Result;

            //Process bulk data
            foreach (bulkCatData bdc in allBDC)
            {
                i_catName = bdc.name;
                i_catDesc = bdc.desc;
                i_catDLan = bdc.langu;

                tmpRet = intAddCat(allCat, i_catName, i_catDLan, i_catDesc);
            }

            await saveCats(tabCats, allCat);

            //Entweder enthält tmpRet eine ID oder ist leer
            return new OkObjectResult(tmpRet);
        }


        public static string intAddCat(fullCat allCat, string i_catName, string i_catDLan, string i_catDesc)
        {
            //Haben wir für diesen Namen schon eine KID?
            string newCatKID = allCat.allIDs.getID(i_catName) ?? "";
            int maxSubCat = 0;

            //Keine KID: wir müssen die Kategorie anlegen
            if (newCatKID == null || newCatKID == "")
            {
                newCatKID = "K" + allCat.allIDs.name2ID.Count.ToString("d3");

                //Ist es eine Unterkategorie?
                if (i_catName.Contains('_'))
                {
                    string[] subCats = i_catName.Split('_');
                    string supCat = string.Join('_', subCats, 0, subCats.Length - 1);
                    string tmpSupCatID = allCat.allIDs.getID(supCat) ?? "";

                    //FAlls wir keine Überkategorie finden brauchen wir keine neue ID -> FEhler
                    newCatKID = "";
                    //Überkategorie gefunden, bauen wir die neu SubKategorie-ID auf
                    if (tmpSupCatID != "")
                    {
                        foreach (string singleKID in allCat.allIDs.name2ID.Values)
                            if (singleKID.StartsWith(tmpSupCatID))
                                maxSubCat++;

                        newCatKID = tmpSupCatID + '.' + maxSubCat.ToString("d3");
                    }
                }
            }
            if (newCatKID != "")
            {
                allCat.allIDs.addN2I(i_catName, newCatKID);
                allCat.allDescs.addDesc(newCatKID, i_catDLan, i_catDesc);
            }

            return newCatKID;
        }

        [FunctionName("addWord")]
        public static async Task<IActionResult> addWord(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            //Offenbar bekommt die Funktion automatisch ein korrekt eingerichtetes Objekt für die Tabelle mitgegeben
            [Table("categorization")] CloudTable tabCats,
            ILogger log)
        {
            string tmpWordID = "";
            fullWord allWords = new fullWord();
            allWords = getWordsData(tabCats).Result;

            string i_fullWord = req.Query["fullWord"];
            i_fullWord = i_fullWord ?? "";

            if (i_fullWord != "" && !allWords.allIDs.name2ID.ContainsKey(i_fullWord))
            {
                tmpWordID = 'W' + (allWords.allIDs.name2ID.Count + 1).ToString("d6");
                allWords.allIDs.addN2I(i_fullWord, tmpWordID);

                await saveWords(tabCats, allWords);
            }
            return new OkObjectResult(tmpWordID);
        }

        [FunctionName("addWordBulk")]
        public static async Task<IActionResult> addWordBulk(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            //Offenbar bekommt die Funktion automatisch ein korrekt eingerichtetes Objekt für die Tabelle mitgegeben
            [Table("categorization")] CloudTable tabCats,
            ILogger log)
        {
            string tmpWordID = "";
            fullWord allWords = new fullWord();
            allWords = getWordsData(tabCats).Result;

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            List<string> bulkWords = JsonConvert.DeserializeObject<List<string>>(requestBody);

            foreach (string oneWord in bulkWords)
            {
                if (!allWords.allIDs.name2ID.ContainsKey(oneWord))
                {

                    tmpWordID = 'W' + (allWords.allIDs.name2ID.Count + 1).ToString("d6");
                    allWords.allIDs.addN2I(oneWord, tmpWordID);
                }
            }
            await saveWords(tabCats, allWords);
            return new OkObjectResult("");
        }


        [FunctionName("catWord")]
        public static async Task<IActionResult> catWord(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            //Offenbar bekommt die Funktion automatisch ein korrekt eingerichtetes Objekt für die Tabelle mitgegeben
            [Table("categorization")] CloudTable tabCats,
            ILogger log)
        {
            string i_WID = req.Query["WID"];
            string i_KID = req.Query["KID"];

            i_KID = i_KID ?? "";
            i_WID = i_WID ?? "";

            if (i_KID != "" && i_WID != "")
            {
                fullWord allWords = getWordsData(tabCats).Result;
                fullCat allCat = getCatsData(tabCats).Result;

                if (allWords.allIDs.name2ID.ContainsValue(i_WID) && allCat.allIDs.name2ID.ContainsValue(i_KID))
                {
                    allCat.allWIDs.addWID(i_KID, i_WID);
                    allWords.allKIDS.addKID(i_WID, i_KID);

                    await saveCats(tabCats, allCat);
                    await saveWords(tabCats, allWords);
                }
            }
            return new OkObjectResult("");
        }

        [FunctionName("catWordBulk")]
        public static async Task<IActionResult> catWordBulk(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            //Offenbar bekommt die Funktion automatisch ein korrekt eingerichtetes Objekt für die Tabelle mitgegeben
            [Table("categorization")] CloudTable tabCats,
            ILogger log)
        {

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            Dictionary<string, string> bulkCW = JsonConvert.DeserializeObject<Dictionary<string, string>>(requestBody);

            fullWord allWords = getWordsData(tabCats).Result;
            fullCat allCat = getCatsData(tabCats).Result;

            foreach (KeyValuePair<string, string> c2w in bulkCW)
            {
                string KID = allCat.allIDs.name2ID[c2w.Key];
                string WID = allWords.allIDs.name2ID[c2w.Value];

                if (allWords.allIDs.name2ID.ContainsKey(WID) && allCat.allIDs.name2ID.ContainsKey(KID))
                {
                    allCat.allWIDs.addWID(KID, WID);
                    allWords.allKIDS.addKID(WID, KID);
                }
            }

            await saveCats(tabCats, allCat);
            await saveWords(tabCats, allWords);
            return new OkObjectResult("");
        }

        private static async Task<fullCat> getCatsData(CloudTable tabCats)
        {
            fullCat tmpRet = new fullCat();

            //Get all category IDs from table
            TableOperation query = TableOperation.Retrieve<catIDs>(beqDef.c_partCat, beqDef.c_rowCatKid);
            TableResult tabRes = await tabCats.ExecuteAsync(query);

            //Entweder bekommen wir welche oder wir fangen mit einer leeren STruktur an
            tmpRet.allIDs = new catIDs();
            if (tabRes.Result != null)
                tmpRet.allIDs = (catIDs)tabRes.Result;

            //Get all category descriptions from table
            query = TableOperation.Retrieve<catDescs>(beqDef.c_partCat, beqDef.c_rowCatDesc);
            tabRes = await tabCats.ExecuteAsync(query);

            //Entweder bekommen wir welche oder wir fangen mit einer leeren STruktur an
            tmpRet.allDescs = new catDescs();
            if (tabRes.Result != null)
                tmpRet.allDescs = (catDescs)tabRes.Result;

            //Get all category WIDs from table
            query = TableOperation.Retrieve<catWIDS>(beqDef.c_partCat, beqDef.c_rowCatWID);
            tabRes = await tabCats.ExecuteAsync(query);

            //Entweder bekommen wir welche oder wir fangen mit einer leeren STruktur an
            tmpRet.allWIDs = new catWIDS();
            if (tabRes.Result != null)
                tmpRet.allWIDs = (catWIDS)tabRes.Result;

            //Reload all data from stored Json-String into classes
            tmpRet.allIDs.reload();
            tmpRet.allDescs.reload();
            tmpRet.allWIDs.reload();

            return tmpRet;
        }

        private static async Task<fullWord> getWordsData(CloudTable tabCats)
        {
            fullWord tmpRet = new fullWord();

            //Get all word IDs from table
            TableOperation query = TableOperation.Retrieve<wordIDs>(beqDef.c_partWord, beqDef.c_rowWordWID);
            TableResult tabRes = await tabCats.ExecuteAsync(query);

            //Entweder bekommen wir welche oder wir fangen mit einer leeren STruktur an
            tmpRet.allIDs = new wordIDs();
            if (tabRes.Result != null)
                tmpRet.allIDs = (wordIDs)tabRes.Result;

            //Get all word KIDs from table
            query = TableOperation.Retrieve<wordKIDS>(beqDef.c_partWord, beqDef.c_rowWordKID);
            tabRes = await tabCats.ExecuteAsync(query);

            //Entweder bekommen wir welche oder wir fangen mit einer leeren STruktur an
            tmpRet.allKIDS = new wordKIDS();
            if (tabRes.Result != null)
                tmpRet.allKIDS = (wordKIDS)tabRes.Result;

            //Reload all data from stored Json-String into classes
            tmpRet.allIDs.reload();
            tmpRet.allKIDS.reload();

            return tmpRet;
        }


        private static async Task<IActionResult> saveCats(CloudTable tabCats, fullCat allCat)
        {
            TableOperation addCat = TableOperation.InsertOrReplace(allCat.allIDs);
            await tabCats.ExecuteAsync(addCat);

            addCat = TableOperation.InsertOrReplace(allCat.allDescs);
            await tabCats.ExecuteAsync(addCat);

            addCat = TableOperation.InsertOrReplace(allCat.allWIDs);
            await tabCats.ExecuteAsync(addCat);

            return new OkObjectResult("");
        }

        private static async Task<IActionResult> saveWords(CloudTable tabCats, fullWord allWord)
        {
            TableOperation addWord = TableOperation.InsertOrReplace(allWord.allIDs);
            await tabCats.ExecuteAsync(addWord);

            addWord = TableOperation.InsertOrReplace(allWord.allKIDS);
            await tabCats.ExecuteAsync(addWord);

            return new OkObjectResult("");
        }

    }

}

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
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
        public const string c_partCatKid = "catKid";

        /// <summary>
        /// Partition Words
        /// </summary>
        public const string c_partWord = "word";

        /// <summary>
        /// Sorting by Categories
        /// </summary>
        public const string c_sortCat = "sortCat";

        /// <summary>
        /// Sorting by Words
        /// </summary>
        public const string c_sortWord = "sortWord";
    }

    public class singleCat
    {
        public string KID;
        public string fN;
        public List<string> WIDs;

        private Dictionary<string, string> aD;

        public string JDescs = "";

        public singleCat(string KID, string fullName)
        {
            this.KID = KID;
            this.fN = fullName;

            this.aD = JsonConvert.DeserializeObject<Dictionary<string, string>>(JDescs);

            if (this.WIDs == null)
                this.WIDs = new List<string>();
            if (this.aD == null)
                this.aD = new Dictionary<string, string>();
        }

        public void addWID(string WID)
        {
            if (!WIDs.Contains(WID))
            {
                WIDs.Add(WID);
            }
        }

        public void addDesc(string lang, string desc)
        {
            if (lang != null && desc != null &&
                lang != "" && desc != "")
            {
                aD[lang] = desc;
                JDescs = JsonConvert.SerializeObject(aD);
            }
        }

    }

    public class allCategs : TableEntity
    {

        public string JsonCats { set; get; } = "";
        public string JsonKidNames { set; get; } = "";

        public int mainCatsCount { set; get; } = 0;

        public Dictionary<string, singleCat> allMyCats;

        /// <summary>
        /// Fullname <> KID
        /// </summary>
        public Dictionary<string, string> allCatsKIDName;

        public allCategs()
        {
            this.PartitionKey = beqDef.c_partCat;
            this.RowKey = beqDef.c_sortCat;
            reLoad();
        }
        public allCategs(string partCat = beqDef.c_partCat, string sortCat = beqDef.c_sortCat)
        {
            this.PartitionKey = partCat;
            this.RowKey = sortCat;

            reLoad();
        }
        public void reLoad()
        {
            this.allMyCats = JsonConvert.DeserializeObject<Dictionary<string, singleCat>>(JsonCats);
            this.allCatsKIDName = JsonConvert.DeserializeObject<Dictionary<string, string>>(JsonKidNames);

            if (this.allMyCats == null)
                this.allMyCats = new Dictionary<string, singleCat>();
            if (this.allCatsKIDName == null)
                this.allCatsKIDName = new Dictionary<string, string>();
        }

        public void reSave()
        {
            JsonCats = JsonConvert.SerializeObject(allMyCats);
            JsonKidNames = JsonConvert.SerializeObject(allCatsKIDName);
        }

        public void addCat(singleCat aCat)
        {
            if (!allMyCats.ContainsKey(aCat.KID))
            {
                allMyCats.Add(aCat.KID, aCat);
                JsonCats = JsonConvert.SerializeObject(allMyCats);

                allCatsKIDName.Add(aCat.fN, aCat.KID);
                JsonKidNames = JsonConvert.SerializeObject(allCatsKIDName);

                if (!aCat.fN.Contains('_'))
                    this.mainCatsCount++;
            }
        }
    }
    public class catNameKID : TableEntity
    {
        public string KID = "";
        public catNameKID(string partCatKid, string catName)
        {
            string l_partCatKid = partCatKid;
            if (partCatKid == "")
                l_partCatKid = beqDef.c_partCatKid;

            this.PartitionKey = l_partCatKid;
            this.RowKey = catName;
        }

        public void addKID(string KID)
        {
            this.KID = KID;
        }
    }

    public class singleWord
    {
        /// <summary>
        /// fullWord
        /// </summary>
        public string fW { get; set; }

        /// <summary>
        /// JsonKids
        /// </summary>
        public string JK { get; set; } = "";
        private List<string> KIDS { get; set; }
        public singleWord(string fullWord)
        {
            this.fW = fullWord;

            this.KIDS = (List<string>)JsonConvert.DeserializeObject(JK);
            if (this.KIDS == null)
                this.KIDS = new List<string>();
        }

        public void addCat(string KID)
        {
            if (KIDS.IndexOf(KID) <= 0)
            {
                KIDS.Add(KID);
                JK = JsonConvert.SerializeObject(KIDS);
            }
        }
    }

    public class allWords : TableEntity
    {
        public Dictionary<string, singleWord> allMyWords { set; get; }

        /// <summary>
        /// Full word <> WID
        /// </summary>
        public Dictionary<string, string> allWordsWidsName { get; set; }

        public string JSonWidsNames { get; set; } = "";

        public int wordCount { get; set; }

        public string JsonWords { get; set; } = "";
        public allWords()
        {
            this.PartitionKey = beqDef.c_partWord;
            this.RowKey = beqDef.c_sortWord;

            reLoad();
        }

        public allWords(string partWord = beqDef.c_partWord, string sortWord = beqDef.c_sortWord)
        {
            this.PartitionKey = partWord;
            this.RowKey = sortWord;

            reLoad();
        }

        public void reLoad()
        {
            allMyWords = JsonConvert.DeserializeObject<Dictionary<string, singleWord>>(JsonWords);
            allWordsWidsName = JsonConvert.DeserializeObject<Dictionary<string, string>>(JSonWidsNames);

            if (allMyWords == null)
                allMyWords = new Dictionary<string, singleWord>();
            if (allWordsWidsName == null)
                allWordsWidsName = new Dictionary<string, string>();
        }

        public void reSave()
        {
            JsonWords = JsonConvert.SerializeObject(allMyWords);
            JSonWidsNames = JsonConvert.SerializeObject(allWordsWidsName);
        }

        public void addWord(string WID, string fullWord)
        {
            if (!allMyWords.ContainsKey(WID))
            {
                singleWord aWord = new singleWord(fullWord);
                allMyWords.Add(WID, aWord);
                JsonWords = JsonConvert.SerializeObject(allMyWords);

                allWordsWidsName.Add(fullWord, WID);
                JSonWidsNames = JsonConvert.SerializeObject(allWordsWidsName);

                this.wordCount++;
            }
        }
    }
    public static class beq_categorization
    {
        [FunctionName("wakeup")]
        public static async Task<IActionResult> wakeup(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
             ILogger log)
        {
            return new OkObjectResult("I'm awake!");
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
            string tmpRet = "No return";
            bool notCreated = false;
            //Prepare to read request body
            string i_catName = req.Query["catName"];
            string i_catDesc = req.Query["catDesc"];
            string i_catDLan = req.Query["catDLan"];

            if (i_catName != null && i_catName != "")
            {

                //Get all categories from table
                var query = TableOperation.Retrieve<allCategs>(beqDef.c_partCat, beqDef.c_sortCat);
                TableResult allCatRes = await tabCats.ExecuteAsync(query);

                //Entweder bekommen wir welche oder wir fangen mit einer leeren STruktur an
                allCategs allCats = new allCategs();
                if (allCatRes.Result != null)
                    allCats = (allCategs)allCatRes.Result;

                allCats.reLoad();

                //Haben wir für diesen Namen schon eine KID?
                string tmpCatKID = "";
                allCats.allCatsKIDName.TryGetValue(i_catName, out tmpCatKID);
                tmpCatKID = tmpCatKID ?? "";

                //Keine KID: wir müssen die Kategorie anlegen
                if (tmpCatKID == "")
                {
                    string newCatID = "001";
                    //Ist das eine Subkategorie?
                    if (i_catName.Contains('_'))
                    {
                        string[] subCats = i_catName.Split('_');
                        string supCat = string.Join('_', subCats, 0, subCats.Length - 1);

                        //Nachsehen ob es die Superkategorie gibt
                        allCats.allCatsKIDName.TryGetValue(supCat, out tmpCatKID);
                        if (tmpCatKID == null || tmpCatKID == "")
                        {
                            //Fehler - Überkategorie existiert nicht - kann nicht weitermachen
                            //(Ich müßte das rekursiv durchsuchen - ich weiß nicht wie ich das hier in C# in einer async Methode machen kann...)
                            tmpRet = "Überkategorie existiert nicht!";
                            notCreated = true;
                        }
                        else
                        {
                            //Überkategorie existiert - wie viele Subkategorien hat es schon?
                            //Wir werden auch die Kategorie selbst finden, daher bei -1 beginnen damit keine Subkats den Zähler auf 0 setzt
                            int maxSubCat = -1;

                            foreach (string singleKID in allCats.allCatsKIDName.Values)
                                if (singleKID.StartsWith(tmpCatKID))
                                    maxSubCat++;

                            newCatID = tmpCatKID + '.' + maxSubCat.ToString("d3");
                        }
                    }
                    else
                    {
                        //Keine Subkategorie - also müssen wir wissen wie viele HAUPTKategorien es schon gibt
                        int tmpMaxCat = allCats.mainCatsCount + 1;
                        newCatID = tmpMaxCat.ToString("d3");
                    }
                    if (notCreated == false)
                    {
                        //Egal ob Subkategorie oder nicht, wir wissen jetzt daß newCatID unsere neue ID enthält
                        singleCat newCat = new singleCat(newCatID, i_catName);
                        newCat.addDesc(i_catDLan, i_catDesc);
                        allCats.addCat(newCat);
                        TableOperation addCat = TableOperation.InsertOrReplace(allCats);
                        await tabCats.ExecuteAsync(addCat);

                        tmpRet = newCatID;
                    }
                }
                else
                {
                    tmpRet = tmpCatKID;
                }
            }

            if (notCreated == false)
                return new OkObjectResult(tmpRet);
            else
                return new UnprocessableEntityObjectResult(tmpRet);
        }

        [FunctionName("addCatDesc")]
        public static async Task<IActionResult> addCatDesc(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            //Offenbar bekommt die Funktion automatisch ein korrekt eingerichtetes Objekt für die Tabelle mitgegeben
            [Table("categorization")] CloudTable tabCats,
            ILogger log)
        {
            string tmpRet = "OK";
            string i_catID = req.Query["catID"];
            string i_catDesc = req.Query["catDesc"];
            string i_catDLan = req.Query["catDLan"];

            if (i_catID != null && i_catDesc != null && i_catDLan != null &&
                i_catID != "" && i_catDesc != "" && i_catDLan != "")
            {
                //Get all categories from table
                var query = TableOperation.Retrieve<allCategs>(beqDef.c_partCat, beqDef.c_sortCat);
                TableResult allCatRes = await tabCats.ExecuteAsync(query);

                //Entweder bekommen wir welche oder wir fangen mit einer leeren STruktur an
                allCategs allCats = new allCategs();
                if (allCatRes.Result != null)
                    allCats = (allCategs)allCatRes.Result;
                allCats.reLoad();

                if (allCats.allMyCats[i_catID] != null)
                    allCats.allMyCats[i_catID].addDesc(i_catDLan, i_catDesc);

                TableOperation chgCat = TableOperation.InsertOrReplace(allCats);
                await tabCats.ExecuteAsync(chgCat);

            }
            return new OkObjectResult(tmpRet);
        }

        [FunctionName("addWord")]
        public static async Task<IActionResult> addWord(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            //Offenbar bekommt die Funktion automatisch ein korrekt eingerichtetes Objekt für die Tabelle mitgegeben
            [Table("categorization")] CloudTable tabCats,
            ILogger log)
        {
            string tmpRet = "OK";
            string i_fullWord = req.Query["fullWord"];

            if (i_fullWord != null && i_fullWord != "")
            {

                //Get all words from table
                var query = TableOperation.Retrieve<allWords>(beqDef.c_partWord, beqDef.c_sortWord);
                TableResult allWordRes = await tabCats.ExecuteAsync(query);

                //Entweder bekommen wir welche oder wir fangen mit einer leeren STruktur an
                allWords allWords = new allWords();
                if (allWordRes.Result != null)
                    allWords = (allWords)allWordRes.Result;
                allWords.reLoad();

                if (!allWords.allWordsWidsName.ContainsKey(i_fullWord))
                {
                    string tmpID = (allWords.wordCount + 1).ToString("d6");
                    allWords.addWord(tmpID, i_fullWord);

                    TableOperation chgWord = TableOperation.InsertOrReplace(allWords);
                    await tabCats.ExecuteAsync(chgWord);

                }
            }
            return new OkObjectResult(tmpRet);
        }

        [FunctionName("addWord2Cat")]
        public static async Task<IActionResult> addWord2Cat(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            //Offenbar bekommt die Funktion automatisch ein korrekt eingerichtetes Objekt für die Tabelle mitgegeben
            [Table("categorization")] CloudTable tabCats,
            ILogger log)
        {
            string tmpRet = "OK";
            string i_catID = req.Query["catID"];
            string i_wordID = req.Query["wordID"];

            if (i_catID != null && i_catID != "" &&
                i_wordID != null && i_wordID != "")
            {

                //Get all words from table
                var query = TableOperation.Retrieve<allWords>(beqDef.c_partWord, beqDef.c_sortWord);
                TableResult allWordRes = await tabCats.ExecuteAsync(query);

                //Entweder bekommen wir welche oder wir fangen mit einer leeren STruktur an
                allWords allWords = new allWords();
                if (allWordRes.Result != null)
                    allWords = (allWords)allWordRes.Result;
                allWords.reLoad();

                //Get all categories from table
                query = TableOperation.Retrieve<allCategs>(beqDef.c_partCat, beqDef.c_sortCat);
                TableResult allCatRes = await tabCats.ExecuteAsync(query);

                //Entweder bekommen wir welche oder wir fangen mit einer leeren STruktur an
                allCategs allCats = new allCategs();
                if (allCatRes.Result != null)
                    allCats = (allCategs)allCatRes.Result;
                allCats.reLoad();

                //Only do something if we now the word and category:
                if (allWords.allMyWords.ContainsKey(i_wordID) && allCats.allMyCats.ContainsKey(i_catID))
                {
                    //Man darf das nicht zusammenfassen?
                    singleWord changeWord = allWords.allMyWords[i_wordID];
                    changeWord.addCat(i_catID);
                    allWords.allMyWords[i_wordID] = changeWord;


                    singleCat changeCat = allCats.allMyCats[i_catID];
                    changeCat.addWID(i_wordID);
                    allCats.allMyCats[i_catID] = changeCat;

                    allCats.reSave();
                    allWords.reSave();

                    TableOperation chgCat = TableOperation.InsertOrReplace(allCats);
                    await tabCats.ExecuteAsync(chgCat);

                    TableOperation chgWord = TableOperation.InsertOrReplace(allWords);
                    await tabCats.ExecuteAsync(chgWord);
                }

            }
            return new OkObjectResult(tmpRet);
        }

        [FunctionName("getJSonData")]
        public static async Task<IActionResult> getJSonData(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            //Offenbar bekommt die Funktion automatisch ein korrekt eingerichtetes Objekt für die Tabelle mitgegeben
            [Table("categorization")] CloudTable tabCats,
            ILogger log)
        {
            string tmpRet = "OK";
            string i_dataID = req.Query["dataID"];

            if (i_dataID == "words")
            {
                //Get all words from table
                var query = TableOperation.Retrieve<allWords>(beqDef.c_partWord, beqDef.c_sortWord);
                TableResult allWordRes = await tabCats.ExecuteAsync(query);

                if (allWordRes.Result != null)
                    tmpRet = ((allWords)allWordRes.Result).JsonWords;

            }
            else if (i_dataID == "categs")
            {
                //Get all categories from table
                var query = TableOperation.Retrieve<allCategs>(beqDef.c_partCat, beqDef.c_sortCat);
                TableResult allCatRes = await tabCats.ExecuteAsync(query);

                if (allCatRes.Result != null)
                   tmpRet = ((allCategs)allCatRes.Result).JsonCats;
            }


            return new OkObjectResult(tmpRet);
        }


    }
}

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
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

    public class catData
    {
        /// <summary>
        /// Langu <> Description
        /// </summary>
        private Dictionary<string, string> catDesc = new Dictionary<string, string>();

        /// <summary>
        /// JSonDesc
        /// </summary>
        public string JD = "";

        private List<string> WIDs = new List<string>();

        /// <summary>
        /// JSonWIDs
        /// </summary>
        public string JW = "";

        public catData()
        {
            reload();
        }

        public void reload()
        {
            catDesc = JsonConvert.DeserializeObject<Dictionary<string, string>>(JD);
            WIDs = JsonConvert.DeserializeObject<List<string>>(JW);

            if (catDesc == null)
                catDesc = new Dictionary<string, string>();
            if (WIDs == null)
                WIDs = new List<string>();
        }

        public void addDesc(string Langu, string Desc)
        {
            catDesc.Add(Langu, Desc);
            JD = JsonConvert.SerializeObject(catDesc);
        }

        public void addWID(string WID)
        {
            WIDs.Add(WID);
            JW = JsonConvert.SerializeObject(WIDs);
        }
    }

    public class allCategs : TableEntity
    {
        /// <summary>
        /// Category Name <> Category ID
        /// </summary>
        public Dictionary<string, string> name2ID = new Dictionary<string, string>();

        private Dictionary<string, catData> ID2Data = new Dictionary<string, catData>();

        public string JSonN2I { get; set; } = "";
        public string JSonI2D { get; set; } = "";

        public int catCount { get; set; } = 0;

        public allCategs()
        {
            this.PartitionKey = beqDef.c_partCat;
            this.RowKey = beqDef.c_sortCat;
            reload();
        }

        public allCategs(string partCat = beqDef.c_partCat, string sortCat = beqDef.c_sortCat)
        {
            this.PartitionKey = partCat;
            this.RowKey = sortCat;
            reload();
        }

        public string getID(string catName)
        {
            string tmpRet = null;
            name2ID.TryGetValue(catName, out tmpRet);
            return tmpRet;
        }

        public void addN2I(string name, string ID)
        {
            name2ID.Add(name, ID);
            JSonN2I = JsonConvert.SerializeObject(name2ID);

            catCount++;
        }

        public void addI2D(string ID, catData newData)
        {
            ID2Data.Add(ID, newData);
            JSonI2D = JsonConvert.SerializeObject(ID2Data);
        }

        public void addW2K(string WID, string KID)
        {
            catData tmpCat = ID2Data[KID];
            if (tmpCat == null)
                tmpCat = new catData();

            tmpCat.addWID(WID);

            ID2Data[KID] = tmpCat;

            JSonI2D = JsonConvert.SerializeObject(ID2Data);
        }

        public void reload()
        {
            name2ID = JsonConvert.DeserializeObject<Dictionary<string, string>>(JSonN2I);
            ID2Data = JsonConvert.DeserializeObject<Dictionary<string, catData>>(JSonI2D);

            if (name2ID == null)
                name2ID = new Dictionary<string, string>();
            if (ID2Data == null)
                ID2Data = new Dictionary<string, catData>();
        }
    }

    public class allWords : TableEntity
    {

        /// <summary>
        /// full word <> word ID
        /// </summary>
        public Dictionary<string, string> name2ID = new Dictionary<string, string>();

        /// <summary>
        /// WID <> Array of KIDs
        /// </summary>
        /// <returns></returns>
        public Dictionary<string, List<string>> WID2KIDS = new Dictionary<string, List<string>>();

        public string JSonN2I { get; set; } = "";

        public string JsonW2K { get; set; } = "";

        public allWords()
        {
            this.PartitionKey = beqDef.c_partWord;
            this.RowKey = beqDef.c_sortWord;

            reload();
        }

        public allWords(string partWord = beqDef.c_partWord, string sortWord = beqDef.c_sortWord)
        {
            this.PartitionKey = partWord;
            this.RowKey = sortWord;

            reload();
        }

        public void reload()
        {
            //JSON konvertierung
            name2ID = JsonConvert.DeserializeObject<Dictionary<string, string>>(JSonN2I);
            if (name2ID == null)
                name2ID = new Dictionary<string, string>();
        }

        public void addWord(string WID, string Word)
        {
            if (!name2ID.ContainsKey(Word))
            {
                name2ID.Add(Word, WID);
                JSonN2I = JsonConvert.SerializeObject(name2ID);
            }
        }

        public void addK2W(string KID, string WID)
        {
            List<string> tmpKIDs;
            WID2KIDS.TryGetValue(WID, out tmpKIDs);
            if (tmpKIDs != null && tmpKIDs.Count > 0)
            {
                tmpKIDs.Add(KID);
                WID2KIDS[WID] = tmpKIDs;
            }
            else
            {
                tmpKIDs = new List<string>();
                tmpKIDs.Add(KID);
                WID2KIDS.Add(WID, tmpKIDs);
            }
            JsonW2K = JsonConvert.SerializeObject(WID2KIDS);
        }

    }

    public static class beq_categorization
    {

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
            string i_catDesc = req.Query["catDesc"];
            string i_catDLan = req.Query["catDLan"];

            if (i_catName != null && i_catName != "")
            {
                if (i_catDesc == null)
                    i_catDesc = "";

                //Get all categories from table
                var query = TableOperation.Retrieve<allCategs>(beqDef.c_partCat, beqDef.c_sortCat);
                TableResult allCatRes = await tabCats.ExecuteAsync(query);

                //Entweder bekommen wir welche oder wir fangen mit einer leeren STruktur an
                allCategs allCats = new allCategs();
                if (allCatRes.Result != null)
                    allCats = (allCategs)allCatRes.Result;

                allCats.reload();

                //Haben wir für diesen Namen schon eine KID?
                string tmpCatKID = allCats.getID(i_catName) ?? "";
                int maxSubCat = 0;

                //Keine KID: wir müssen die Kategorie anlegen
                if (tmpCatKID == null || tmpCatKID == "")
                {
                    string newCatID = "K001";

                    //Ist es eine Unterkategorie?
                    if (i_catName.Contains('_'))
                    {
                        string[] subCats = i_catName.Split('_');
                        string supCat = string.Join('_', subCats, 0, subCats.Length - 1);
                        string tmpSupCatID = allCats.getID(supCat) ?? "";

                        //FAlls wir keine Überkategorie finden brauchen wir keine neue ID -> FEhler
                        newCatID = "";
                        //Überkategorie gefunden, bauen wir die neu SubKategorie-ID auf
                        if (tmpSupCatID != "")
                        {
                            foreach (string singleKID in allCats.name2ID.Values)
                                if (singleKID.StartsWith(supCat))
                                    maxSubCat++;

                            newCatID = tmpSupCatID + '.' + maxSubCat.ToString("d3");
                        }
                    }

                    if (newCatID != "")
                    {
                        allCats.addN2I(i_catName, newCatID);
                        catData newCD = new catData();
                        newCD.addDesc(i_catDLan, i_catDesc);
                        allCats.addI2D(newCatID, newCD);

                        TableOperation addCat = TableOperation.InsertOrReplace(allCats);
                        await tabCats.ExecuteAsync(addCat);
                    }

                    tmpRet = newCatID;
                }
                else
                {
                    tmpRet = tmpCatKID;
                }
            }

            //Entweder enthält tmpRet eine ID oder ist leer
            return new OkObjectResult(tmpRet);
        }

        [FunctionName("addWord")]
        public static async Task<IActionResult> addWord(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
        //Offenbar bekommt die Funktion automatisch ein korrekt eingerichtetes Objekt für die Tabelle mitgegeben
        [Table("categorization")] CloudTable tabCats,
        ILogger log)
        {
            string tmpRet = "";

            string i_fullWord = req.Query["fullWord"];

            if (i_fullWord != null && i_fullWord != "")
            {
                //Get all categories from table
                var query = TableOperation.Retrieve<allWords>(beqDef.c_partWord, beqDef.c_sortWord);
                TableResult allWordRes = await tabCats.ExecuteAsync(query);
                //Entweder bekommen wir welche oder wir fangen mit einer leeren STruktur an
                allWords allWord = new allWords();
                if (allWordRes.Result != null)
                    allWord = (allWords)allWordRes.Result;

                allWord.reload();

                string tmpWID = "W" + allWord.name2ID.Count.ToString("d3");

                allWord.addWord(tmpWID, i_fullWord);

                TableOperation addWord = TableOperation.InsertOrReplace(allWord);
                await tabCats.ExecuteAsync(addWord);
            }

            //Entweder enthält tmpRet eine ID oder ist leer
            return new OkObjectResult(tmpRet);
        }

        [FunctionName("catWord")]
        public static async Task<IActionResult> catWord(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
        //Offenbar bekommt die Funktion automatisch ein korrekt eingerichtetes Objekt für die Tabelle mitgegeben
        [Table("categorization")] CloudTable tabCats,
        ILogger log)
        {
            string tmpRet = "";

            string i_WID = req.Query["WID"];
            string i_KID = req.Query["KID"];

            i_WID = i_WID ?? "";
            i_KID = i_KID ?? "";

            if (i_KID != "" && i_WID != "")
            {
                //Get all categories from table
                var query = TableOperation.Retrieve<allWords>(beqDef.c_partWord, beqDef.c_sortWord);
                TableResult allWordRes = await tabCats.ExecuteAsync(query);
                //Entweder bekommen wir welche oder wir fangen mit einer leeren STruktur an
                allWords allWord = new allWords();
                if (allWordRes.Result != null)
                    allWord = (allWords)allWordRes.Result;
                allWord.reload();

                //Get all categories from table
                query = TableOperation.Retrieve<allCategs>(beqDef.c_partCat, beqDef.c_sortCat);
                TableResult allCatRes = await tabCats.ExecuteAsync(query);

                //Entweder bekommen wir welche oder wir fangen mit einer leeren STruktur an
                allCategs allCats = new allCategs();
                if (allCatRes.Result != null)
                    allCats = (allCategs)allCatRes.Result;
                allCats.reload();

                //Do we know the KID?
                if (allCats.name2ID.ContainsValue(i_KID) && allWord.name2ID.ContainsValue(i_WID))
                {
                    allWord.addK2W(i_KID, i_WID);
                    allCats.addW2K(i_WID, i_KID);
                }

                TableOperation addCat = TableOperation.InsertOrReplace(allCats);
                await tabCats.ExecuteAsync(addCat);
                TableOperation addWord = TableOperation.InsertOrReplace(allWord);
                await tabCats.ExecuteAsync(addWord);
            }

            return new OkObjectResult(tmpRet);
        }




        /*
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
        */
    }

}

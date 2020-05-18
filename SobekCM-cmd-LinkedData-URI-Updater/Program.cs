using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Xml;
using System.Xml.Linq;
using disUtility;
using SobekCM.Resource_Object;
using SobekCM.Resource_Object.Bib_Info;
using VDS.RDF.Parsing;
using VDS.RDF.Query;

namespace SobekCM_cmd_LinkedData_URI_Updater
{
    class Program
    {
        private static string myversion = "20200518-2124";

        private class item
        {
            public string packageid { get; set; }
            public string mainthumbnail { get; set; }
            public string loi { get; set; }
            public string path_folder { get; set; }
            public string url_folder { get; set; }
        }

        static void Main(string[] args)
        {
            if (args.Length==0)
            {
                Console.WriteLine("Command line arguments that are available: prefix, aggcode, db...");
                return;
            }
            else
            {
                Console.WriteLine("version=[" + myversion + "].");
            }

            string prefix = null,aggcode=null, db=null;
            Boolean mytry = false, urlmode=false;

            foreach (string myArg in args)
            {
                if (myArg.StartsWith("--prefix"))
                {
                    prefix = myArg.Substring(9);
                }

                if (myArg.StartsWith("--aggcode"))
                {
                    aggcode = myArg.Substring(10);
                }

                if (myArg.StartsWith("--db"))
                {
                    db = myArg.Substring(5);
                }

                if (myArg.StartsWith("--urlmode"))
                {
                    urlmode = true;
                }
            }

            string myuri = "http://fuseki.dss-test.org:3030/authoritiessubjects-madsrdf-ttl-1/query";
            RemoteQueryProcessor rqp = sparql.Get_Remote_Query_Processor(myuri);

            SparqlQueryParser parser = new SparqlQueryParser();
            SparqlQuery q=null;
            

            // ref SparqlQuery q, ref SparqlQueryParser parser

            string statement = null, path_mets=null, path_mets_xsd=null, url_mets=null;

            path_mets_xsd = xmlUtilities.Get_METS_xsd_path();

            if (db == null) db = "liveaws";
            SqlConnection conn = mssql.getMSSQLconnection(null, db);
            int idx = 0;
            
            //MySql.Data.MySqlClient.MySqlConnection mconn = mysql.getMySQLconn("ldc_digitization_aws");

            statement = "select Bibid,VID,MainThumbnail from SobekCM_Item as i join SobekCM_Item_Group as ig ";
            statement += "on i.groupid=ig.groupid ";
            statement += "where i.deleted=0 and i.dark=0 and i.ip_restriction_mask=0";

            if (aggcode!=null)
            {
                statement += " and aggregationcodes like '%" + aggcode + "%' ";
            }

            statement += "order by bibid,vid";

            //MySql.Data.MySqlClient.MySqlDataReader mdr = mysql.getMySQLdataReader(mconn, statement);

            SqlDataReader dr = mssql.getMSSQLdataReader(statement, conn);

            List<item> items = new List<item>();
            
            if (dr!=null && dr.HasRows)
            {
                while (dr.Read())
                {
                    item myitem = new item();
                    myitem.packageid = dr["BibID"].ToString().Trim() + "_" + dr["VID"].ToString().Trim();
                    myitem.mainthumbnail = dr["MainThumbnail"].ToString().Trim();

                    if (dr["MainThumbnail"].ToString().Trim().Length >= 9)
                    {
                        myitem.loi = dr["MainThumbnail"].ToString().Trim().Substring(0, 9);
                    }
                    
                    myitem.path_folder = sobekcm.GetContentFolderPathFromPackageID(myitem.packageid);
                    myitem.url_folder = sobekcm.GetContentFolderURLfromPackageID(myitem.packageid);
                    items.Add(myitem);
                }

                dr.Close();

                Console.WriteLine(items.Count + " records were read.");

                foreach (item myitem in items)
                {
                    idx++;

                    if (urlmode)
                    {
                        path_mets = Path.GetTempPath() + @"\" + myitem.packageid + ".mets.xml";
                        url_mets = myitem.url_folder + myitem.packageid + ".mets.xml";
                        Console.WriteLine("urlmode: getting url=[" + url_mets + "] and writing to path_mets=[" + path_mets + "].");
                        xmlUtilities.WriteMETSviaURLtoPath(url_mets, path_mets);
                    }
                    else
                    {
                        path_mets = prefix + myitem.path_folder + myitem.packageid + ".mets.xml";
                    }
                    
                    Console.WriteLine("\r\n");

                    if (File.Exists(path_mets))
                    {
                        Console.WriteLine(idx + ". " + myitem.packageid + " mets exists.");

                        Console.WriteLine("Trying to separately validate file ");
                        mytry = xmlUtilities.validateXML("http://www.loc.gov/METS/", path_mets_xsd, path_mets);
                        
                        if (mytry)
                        {
                            Console.WriteLine("METS xml is Valid.");

                            find_exact_matches_update_mets(path_mets, ref q, ref parser, ref rqp);
                        }
                        else
                        {
                            Console.WriteLine("METS xml is Invalid.");
                        }
                    }
                    else
                    {
                        Console.WriteLine(idx + ". " + myitem.packageid + " mets does NOT exist [" + path_mets + "].");
                    }
                }
            }
        }

        private static void find_exact_matches_update_mets(string path_mets, ref SparqlQuery q, ref SparqlQueryParser parser, ref RemoteQueryProcessor rqp)
        {
            XmlDocument doc = new XmlDocument();
            XmlNodeList nodes;
            XmlAttributeCollection attrs;

            string term = null, query=null;

            //SobekCM_Item item = new SobekCM_Item();

            Console.WriteLine("FEMUM: Reading [" + path_mets + "].");
            
            //item.Read_From_METS(path_mets);

            /*      
            if (item.Contains_Complex_Content)
            {
                Console.WriteLine("Contains complex content.");
            }
            else
            {
                Console.WriteLine("Does NOT contain complex content.");
            }

            if (item.hasBibliographicData)
            {
                Console.WriteLine("Has bibliographicdata.");
            }
            else
            {
                Console.WriteLine("Does NOT have bibliographicdata.");
            }
            */
            
            /*
            if (item.Bib_Info.Subjects_Count>0)
            {
                Console.WriteLine("There are [" + item.Bib_Info.Subjects.Count + "] subjects.");

                foreach (Subject_Info si in item.Bib_Info.Subjects)
                {
                    Console.WriteLine("actual_id=[" + si.Actual_ID + "].");
                    Console.WriteLine("id=[" + si.ID + "].");
                    Console.WriteLine("authority=[" + si.Authority + "].");
                    Console.WriteLine("class_type=[" + si.Class_Type + "].");
                    Console.WriteLine("language=[" + si.Language + "].");
                    Console.WriteLine("\r\n");
                }
            }
            else
            {
                Console.WriteLine("There are no subjects.");
            }
            */

            //item = null;
           
            XmlNamespaceManager mynsm = new XmlNamespaceManager(doc.NameTable);
            mynsm.AddNamespace("METS", "http://www.loc.gov/METS/");
            mynsm.AddNamespace("mods", "http://www.loc.gov/mods/v3");

            List<sparql.sqresult> sqresults = new List<sparql.sqresult>();

            doc.Load(path_mets);
            
            nodes = doc.SelectNodes("//mods:subject", mynsm);

            if (nodes!=null && nodes.Count>0)
            {
                Console.WriteLine("There are [" + nodes.Count + "] subjects.\r\n");

                foreach (XmlNode node in nodes)
                {
                    attrs = node.Attributes;
                    Console.WriteLine("\t" + node.FirstChild.InnerText + ": id=[" + attrs["ID"].Value + "], authority=[" + attrs["authority"].Value + "].");

                    term = node.FirstChild.InnerText.ToLower();
                    //Console.WriteLine("Getting query for term=[" + term + "].");
                    query=Get_sh_query(term);
                    q = parser.ParseFromString(query);
                    //Console.WriteLine(query);
                    //Console.WriteLine("myuri=[" + myuri + "].");
                    sqresults = sparql.GetSparqlQueryResults(ref q, ref rqp);

                    if (sqresults!=null && sqresults.Count==1)
                    {
                        Console.WriteLine("Retrieved URI=[" + sqresults[0].s + "].");
                    }
                    else if (sqresults!=null && sqresults.Count>1)
                    {
                        Console.WriteLine("There were [" + sqresults.Count + "] results.");
                    }
                    else
                    {
                        Console.WriteLine("There were NO results for [" + term + "].");
                    }

                    Console.WriteLine("\r\n");
                }
            }
            else
            {
                Console.WriteLine("There are NO subjects.");
            }
        }

        private static string Get_sh_query(string term)
        {
            string query = @"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                PREFIX lc: <http://semweb.mmlab.be/ns/linkedconnections#>

                SELECT ?subject ?predicate ?object

                WHERE 
                {
                    ?subject <http://www.loc.gov/mads/rdf/v1#authoritativeLabel> ?object .
                    ?subject <http://www.loc.gov/mads/rdf/v1#isMemberOfMADSCollection> <http://id.loc.gov/authorities/subjects/collection_LCSHAuthorizedHeadings> .
                    FILTER(lcase(str(?object)) in ('" + term + @"')) .
                }
            ";

            return query;
        }
    }
}
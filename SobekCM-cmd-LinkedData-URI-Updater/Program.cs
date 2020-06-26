using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Xml;
using disUtility;
using VDS.RDF.Parsing;
using VDS.RDF.Query;

namespace SobekCM_cmd_LinkedData_URI_Updater
{
    class Program
    {
        private static string myversion = "20200626-1324";

        private class item
        {
            public string packageid { get; set; }
            public string mainthumbnail { get; set; }
            public string loi { get; set; }
            public string path_folder { get; set; }
            public string url_folder { get; set; }
        }

        private static void Main(string[] args)
        {
            if (args.Length==0)
            {
                Console.WriteLine("\r\n\r\nCommand line arguments that are available (some required, some optional):\r\n\r\n--prefix= (non-urlmode, drive/path to content folder, while running on server)\r\n--aggcode=\r\n--db= (optional, defaults to live database, nickname of sobekcm database server/instance/database)\r\n--urlmode (optional, test mode, to retrieve files via URL and write updated files locally vs. accessing local file system to overwrite live files)\r\n--limit= (optional, to limit the number of records to update (urlmode or not).\r\n");
                return;
            }
            else
            {
                Console.WriteLine("version=[" + myversion + "].");
            }

            string prefix = null,aggcode=null, db=null;
            Boolean mytry = false, urlmode=false;
            int limit = 999;

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

                if (myArg.StartsWith("--limit"))
                {
                    limit = int.Parse(myArg.Substring(8));
                }
            }

            string myuri = "http://fuseki.dss-test.org:3030/authoritiessubjects-madsrdf-ttl-1/query";

            // using dotNetRDF to connect to Apache Jena Fuseki tdb
            RemoteQueryProcessor rqp = sparql.Get_Remote_Query_Processor(myuri);
            SparqlQueryParser parser = new SparqlQueryParser();
            SparqlQuery q=null;
        
            string statement = null, path_mets=null, path_mets_xsd=null, url_mets=null;

            // xmlUtilities is a helper class for xml operations in the disUtility library
            path_mets_xsd = xmlUtilities.Get_METS_xsd_path();

            if (db == null) db = "liveaws";
            // mssql is a helper class for working with a Microsoft SQL Server in the disUtility library
            SqlConnection conn = mssql.getMSSQLconnection(null, db);
            int idx = 0;

            // connecting to database to get publicly available packageids. Could just as easily use the publicly available OAI data provider - https://digital.lib.usf.edu//sobekcm_oai.aspx?verb=Identify&verb=Identify, etc.

            statement = "select Bibid,VID,MainThumbnail from SobekCM_Item as i join SobekCM_Item_Group as ig ";
            statement += "on i.groupid=ig.groupid ";
            statement += "where i.deleted=0 and i.dark=0 and i.ip_restriction_mask=0";

            if (aggcode!=null)
            {
                statement += " and aggregationcodes like '%" + aggcode + "%' ";
            }

            statement += "order by bibid,vid";

            SqlDataReader dr = mssql.getMSSQLdataReader(statement, conn);

            List<item> items = new List<item>();
            
            if (dr!=null && dr.HasRows)
            {
                while (dr.Read())
                {
                    idx++;
                    
                    if (idx > limit) break;

                    item myitem = new item();
                    myitem.packageid = dr["BibID"].ToString().Trim() + "_" + dr["VID"].ToString().Trim();
                    myitem.mainthumbnail = dr["MainThumbnail"].ToString().Trim();

                    if (dr["MainThumbnail"].ToString().Trim().Length >= 9)
                    {
                        myitem.loi = dr["MainThumbnail"].ToString().Trim().Substring(0, 9);
                    }
                    
                    // sobekcm is a helper class for working with a SobekCM-based repository in the disUtility library
                    myitem.path_folder = sobekcm.GetContentFolderPathFromPackageID(myitem.packageid);
                    myitem.url_folder = sobekcm.GetContentFolderURLfromPackageID(myitem.packageid);
                    items.Add(myitem);
                }

                dr.Close();

                Console.WriteLine(items.Count + " records were read.");
                idx = 0;

                foreach (item myitem in items)
                {
                    idx++;

                    if (urlmode)
                    {
                        // running locally for testing, downloading mets via URL - https://hostname/content/SF/S0/##/##/##/#####/SFS#######.mets.xml
                        // A packageid consists of a SobekCM BibID and VID in either the form bibid_vid (or a 'did' in solr - bibid:vid)
                        // the bibid is a 10 digit alphanumeric: a alpha prefix (ours is SFS) and an index number, and a 5 digit VID, both with leading zeros.
                        // So packageid SFS0070901_00001 has a mets URL of https://digital.lib.usf.edu/content/SF/S0/07/09/01/00001/SFS070901_00001.mets.xml

                        path_mets = Path.GetTempPath() + @"\" + myitem.packageid + ".mets.xml";
                        url_mets = myitem.url_folder + myitem.packageid + ".mets.xml";
                        Console.WriteLine("urlmode: getting url=[" + url_mets + "] and writing to path_mets=[" + path_mets + "].");
                        xmlUtilities.WriteMETSviaURLtoPath(url_mets, path_mets);
                        string path_backup = path_mets + ".bak";
                        File.WriteAllText(path_backup, File.ReadAllText(path_mets));
                    }
                    else
                    {
                        // running on SobkeCM server (live, test, or dev)
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
                            Console.WriteLine("original METS xml is Valid, proceeding with update.");

                            find_exact_matches_update_mets(myitem.packageid, path_mets, path_mets_xsd, ref q, ref parser, ref rqp);
                        }
                        else
                        {
                            Console.WriteLine("METS xml is Invalid, skipping processing.");
                        }
                    }
                    else
                    {
                        Console.WriteLine(idx + ". " + myitem.packageid + " metsxml file does NOT exist [" + path_mets + "], skipping processing.");
                    }

                    Console.WriteLine("________________________________________________________");
                }
            }
        }

        private static void find_exact_matches_update_mets(string packageid,string path_mets, string path_mets_xsd, ref SparqlQuery q, ref SparqlQueryParser parser, ref RemoteQueryProcessor rqp)
        {
            XmlDocument doc = new XmlDocument();
            XmlNodeList nodes;
            XmlNode node2;
            XmlAttributeCollection attrs;

            string term = null, query=null, id=null;
            Boolean noID = false;
            int idx = 0;

            Console.WriteLine("FEMUM: Reading metsxml file, path=[" + path_mets + "].");
           
            XmlNamespaceManager mynsm = new XmlNamespaceManager(doc.NameTable);
            mynsm.AddNamespace("METS", "http://www.loc.gov/METS/");
            mynsm.AddNamespace("mods", "http://www.loc.gov/mods/v3");
            string authorityURI = "http://id.loc.gov/authorities/subjects";

            List<sparql.sqresult> sqresults = new List<sparql.sqresult>();

            Dictionary<string, string> subjectupdates = new Dictionary<string, string>();

            doc.Load(path_mets);
            
            nodes = doc.SelectNodes("//mods:subject", mynsm);

            if (nodes!=null && nodes.Count>0)
            {
                Console.WriteLine("There are [" + nodes.Count + "] subjects for [" + packageid + "].\r\n");

                foreach (XmlNode node in nodes)
                {
                    idx++;

                    attrs = node.Attributes;
                    Console.Write("\t" + node.FirstChild.InnerText + ":");

                    try
                    {
                        Console.Write("id=[" + attrs["ID"].Value + "],");
                        id = attrs["ID"].ToString();
                    }
                    catch (Exception)
                    {
                        Console.Write("id=[N/A] => " + idx + ",");
                        id = idx.ToString();
                    }

                    try
                    {
                        Console.Write("authority=[" + attrs["authority"].Value + "].\r\n");
                    }
                    catch (Exception)
                    {
                        Console.Write("authority=[N/A]).\r\n");
                    }

                    term = node.FirstChild.InnerText.ToLower();
                    term = term.Replace("'", "%27");
                    term = term.Replace("\"", "%22");

                    query=Get_sh_query(term);
                    q = parser.ParseFromString(query);
                    sqresults = sparql.GetSparqlQueryResults(ref q, ref rqp);

                    if (sqresults!=null && sqresults.Count==1)
                    {
                        Console.WriteLine("Retrieved 1 URI=[" + sqresults[0].s + "] for [" + term + "], id=[" + id + "].");
                        subjectupdates.Add(id, sqresults[0].s.ToString());
                    }
                    else if (sqresults!=null && sqresults.Count>1)
                    {
                        Console.WriteLine("There were [" + sqresults.Count + "] results for [" + term + "], id=[" + id + "].");
                    }
                    else
                    {
                        Console.WriteLine("There were NO results for [" + term + "], id=[" + id + "].");
                    }

                    Console.WriteLine("\r\n");
                }

                XmlAttribute attr;

                Console.WriteLine(subjectupdates.Count + " URIs found for [" + packageid + "], processing updates.");

                foreach (KeyValuePair<string, string> subjectupdate in subjectupdates)
                {
                    Console.WriteLine(packageid + ": " + subjectupdate.Key + "=<" + subjectupdate.Value + ">");
                }

                Console.WriteLine("\r\n");

                foreach (KeyValuePair<string,string> subjectupdate in subjectupdates)
                {
                    Console.WriteLine(packageid + ": " + subjectupdate.Key + "=<" + subjectupdate.Value + ">");

                    // add id attribute if it is missing
                    /*
                    if (int.TryParse(subjectupdate.Key, out int newid))
                    {
                        Console.WriteLine("Replacing missing ID attribute for subject position=" + subjectupdate.Key);
                        node2 = doc.SelectSingleNode("//mods:subject[position()=" + idx + "]",mynsm);
                        attr = doc.CreateAttribute("ID");
                        attr.Value = subjectupdate.Key;
                        node2.Attributes.SetNamedItem(attr);
                    }
                    */

                    Console.WriteLine("Trying to update subject[@ID='" + subjectupdate.Key + "']/position=" + subjectupdate.Key);

                    if (int.TryParse(subjectupdate.Key, out int newid2))
                    {
                        node2 = doc.SelectSingleNode("//mods:subject[position()=" + subjectupdate.Key + "]",mynsm);
                    }
                    else
                    {
                        node2 = doc.SelectSingleNode("//mods:subject[@ID='" + subjectupdate.Key + "']", mynsm);
                    }

                    attr = doc.CreateAttribute("authorityURI");
                    attr.Value = authorityURI;
                    node2.Attributes.SetNamedItem(attr);

                    attr = doc.CreateAttribute("valueURI");
                    attr.Value = subjectupdate.Value;
                    node2.Attributes.SetNamedItem(attr);
                }

                Console.WriteLine("Updating lastmoddate for [" + packageid + "].");
                node2 = doc.SelectSingleNode("//METS:metsHdr", mynsm);
                attr = doc.CreateAttribute("LASTMODDATE");
                attr.Value = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ssZ");
                node2.Attributes.SetNamedItem(attr);

                //Console.WriteLine("\r\n\r\n" + doc.OuterXml.ToString() + "\r\n\r\n");

                doc.Save(path_mets);
                Console.WriteLine("Saved updated mets to [" + path_mets + "].");

                Boolean mytry = xmlUtilities.validateXML("http://www.loc.gov/METS/", path_mets_xsd, path_mets);

                if (mytry)
                {
                    Console.WriteLine("Valid after save [" + path_mets + "].");
                }
                else
                {
                    Console.WriteLine("Invalid after save [" + path_mets + "].");
                }

                doc = null;
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
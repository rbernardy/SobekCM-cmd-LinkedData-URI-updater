using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using disUtility;
using SobekCM.Resource_Object;
using SobekCM.Resource_Object.Bib_Info;

namespace SobekCM_cmd_LinkedData_URI_Updater
{
    class Program
    {
        private static string myversion = "20200517-0009";

        private class item
        {
            public string packageid { get; set; }
            public string mainthumbnail { get; set; }
            public string loi { get; set; }
            public string path_folder { get; set; }
        }

        static void Main(string[] args)
        {
            if (args.Length==0)
            {
                Console.WriteLine("Command line arguments are required: ...");
                return;
            }
            else
            {
                Console.WriteLine("version=[" + myversion + "].");
            }

            string prefix = null,aggcode=null;

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

            }

            string statement = null, path_mets=null;
            SqlConnection conn = mssql.getMSSQLconnection(null, "liveaws");
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
                    items.Add(myitem);
                }

                dr.Close();

                Console.WriteLine(items.Count + " records were read.");

                foreach (item myitem in items)
                {
                    idx++;

                    path_mets = prefix + myitem.path_folder + myitem.packageid + ".mets.xml";

                    Console.WriteLine("\r\n");

                    if (File.Exists(path_mets))
                    {
                        Console.WriteLine(idx + ". " + myitem.packageid + " mets exists.");

                        find_exact_matches_update_mets(path_mets);
                    }
                    else
                    {
                        Console.WriteLine(idx + ". " + myitem.packageid + " mets does NOT exist [" + path_mets + "].");
                    }
                }
            }
        }

        private static void find_exact_matches_update_mets(string path_mets)
        {
            SobekCM_Item item = new SobekCM_Item();

            Console.WriteLine("FEMUM: Reading [" + path_mets + "].");
            
            item.Read_From_METS(path_mets);
                        
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
                Console.WriteLine("There are no subjects [" + path_mets + "], [" + item.BibID + "_" + item.VID + "], [" + item.Bib_Title + "].");
            }

            item = null;
        }
    }
}
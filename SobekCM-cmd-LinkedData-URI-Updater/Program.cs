using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using disUtility;

namespace SobekCM_cmd_LinkedData_URI_Updater
{
    class Program
    {
        private static string myversion = "20200516-1816";

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

            string statement = null, path_mets=null;
            SqlConnection conn = mssql.getMSSQLconnection(null, "liveaws");
            int idx = 0;
            
            //MySql.Data.MySqlClient.MySqlConnection mconn = mysql.getMySQLconn("ldc_digitization_aws");

            statement = "select Bibid,VID,MainThumbnail from SobekCM_Item as i join SobekCM_Item_Group as ig ";
            statement += " on i.groupid=ig.groupid and i.deleted=0 and i.dark=0 and i.ip_restriction_mask=0 order by bibid,vid";

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

                    path_mets = myitem.path_folder + myitem.packageid + ".mets.xml";

                    if (File.Exists(path_mets))
                    {
                        Console.WriteLine(idx + ". " + myitem.packageid + " mets exists.");
                    }
                    else
                    {
                        Console.WriteLine(idx + ". " + myitem.packageid + " mets does NOT exist [" + path_mets + "].");
                    }
                }
            }
        }
    }
}
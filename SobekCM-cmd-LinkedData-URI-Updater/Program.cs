using System;
using System.Collections.Generic;
using System.IO;
using disUtility;

namespace SobekCM_cmd_LinkedData_URI_Updater
{
    class Program
    {
        private static string myversion = "20200516-1606";

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
            disUtility.mysql mysql = new mysql();
            MySql.Data.MySqlClient.MySqlConnection mconn = mysql.getMySQLconn("ldc_digitization_aws");

            statement = "select Bibid,VID,mainthumbnail from SobekCM_Item as i join SobekCM_Item_Group as ig ";
            statement += " on i.groupid=ig.groupid and i.deleted=0 and i.dark=0 and i.ip_restriction_mask=0 order by bibid,vid";

            MySql.Data.MySqlClient.MySqlDataReader mdr = mysql.getMySQLdataReader(mconn, statement);

            List<item> items = new List<item>();
            
            if (mdr!=null && mdr.HasRows)
            {
                while (mdr.Read())
                {
                    item myitem = new item();
                    myitem.packageid = mdr["bibid"].ToString().Trim() + "_" + mdr["vid"].ToString().Trim();
                    myitem.mainthumbnail = mdr["mainthumbnail"].ToString().Trim();
                    myitem.loi = mdr["mainthumbnail"].ToString().Trim().Substring(0, 9);
                    myitem.path_folder = sobekcm.GetContentFolderPathFromPackageID(myitem.packageid);
                    items.Add(myitem);
                }

                mdr.Close();

                Console.WriteLine(items.Count + " records were read.");

                foreach (item myitem in items)
                {
                    path_mets = myitem.path_folder + myitem.packageid + ".mets.xml";

                    if (File.Exists(path_mets))
                    {
                        Console.WriteLine(myitem.packageid + " mets exists.");
                    }
                    else
                    {
                        Console.WriteLine(myitem.packageid + " mets does NOT exist [" + path_mets + "].");
                    }
                }
            }
        }
    }
}
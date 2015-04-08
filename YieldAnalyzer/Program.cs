using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;


namespace YieldAnalyzer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("你好 Welcome");
            Console.ReadLine();

            int r = 2477;

            DataTable mytable = new DataTable();

            mytable = SQLServer.READdt();

        }
    }

    class SQLServer {
        static public DataTable READdt()
        {

            string query = "SELECT TOP(" + ") " +
                                     "[idNearByTimeLine]" +
                                    ",[SeasonID]" +
                                    ",[FieldID]" +
                                    ",[FieldGroupID]" +
                                    ",[createdAT]" +
                                    ",[msgID]" +
                                    ",[msgmid]" +
                                    ",[msgidstr]" +
                                    ",[msgtext]" +
                                    ",[msgin_reply_to_status_id]" +
                                    ",[msgin_reply_to_user_id]" +
                                    ",[msgin_reply_to_screen_name]" +
                                    ",[msgfavorited]" +
                                    ",[msgsource]" +
                                    ",[geoTYPE]" +
                                    ",[geoLAT]" +
                                    ",[geoLOG]" +
                                    ",[distance]" +
                                    ",[userID]" +
                                    ",[userscreen_name]" +
                                    ",[userprovince]" +
                                    ",[usercity]" +
                                    ",[userlocation]" +
                                    ",[userdescription]" +
                                    ",[userfollowers_count]" +
                                    ",[userfriends_count]" +
                                    ",[userstatuses_count]" +
                                    ",[userfavourites_count]" +
                                    ",[usercreated_at]" +
                                    ",[usergeo_enabled]" +
                                    ",[userverified]" +
                                    ",[userbi_followers_count]" +
                                    ",[userlang]" +
                                    ",[userclient_mblogid]" +
                                    ",[nearbytimelinecol]" +
                                    ",[RowADDEDtime]" +
                                    ",[WGSLatitudeX]" +
                                    ",[WGSLongitudeY]" +
                                    ",[location]" +
                                    " FROM [weibotest2].[dbo].[NBT2] Where [idNearByTimeLine] > " + id.ToString() + " order by [idNearByTimeLine] ASC;";


            //CONNECT
            SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.MSSQL);
            try
            {
                myConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }


            var table = new DataTable();
            using (var da = new SqlDataAdapter(query, myConnection))
            {
                da.Fill(table);
            }

            myConnection.Close();

            return table;


        }
    }
}

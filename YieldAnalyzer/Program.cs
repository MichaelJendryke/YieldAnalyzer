using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Transactions;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using Microsoft.SqlServer.Types;
using System.Text.RegularExpressions;



//using Calculations;
//using ProSQLSpatial;

namespace YieldAnalyzer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine(DateTime.Now + " 你好 Welcome");
            int process = 4;

            string query = "";
            DataTable PolyTable = new DataTable();
            DataTable PointTable = new DataTable();
            DataTable PointTableEnv = new DataTable();
            switch (process)
            {



                case 1: //Intersecting points with Polygon
                    Console.WriteLine("process 1: Update Points with ID from intersecting Polygon");
                    Console.WriteLine(DateTime.Now + " Start to load PolyTable with:");
                    query = "SELECT OBJECTID, Shape from weiboDEV.dbo.GADM_CHN_ADM3_SINGLE";
                    //Console.WriteLine(query);
                    PolyTable = SQLServer.readDT(query);
                    //disp.dispDT(PolyTable);
                    Console.WriteLine("PolyTable has " + PolyTable.Rows.Count + " records");


                    int count = 0;
                    for (Int32 i = 406; i < PolyTable.Rows.Count; i++)
                    {
                        string poly = PolyTable.Rows[i]["Shape"].ToString();
                        //Console.WriteLine(PolyTable.Rows[i]["OBJECTID"].GetType());
                        Int32 polyID = (Int32)PolyTable.Rows[i]["OBJECTID"];


                        Console.Write("At polyID: " + polyID.ToString() + "\t");


                        double buffer = 0.0001;
                        var dims = Calculations.GetMaxMinXYFromPolygonstring(poly, buffer);
                        //Console.Write(PolyTable.Rows[i]["OBJECTID"].ToString() + " ");
                        //Console.WriteLine(dims);


                        query = "Select [msgID],[location] from [weiboDEV].[dbo].[GEOminimal] WHERE [WGSLongitudeY] > " + dims.Item1.ToString() + " AND [WGSLongitudeY] < " + dims.Item2.ToString() + " AND [WGSLatitudeX] > " + dims.Item3.ToString() + " AND [WGSLatitudeX] < " + dims.Item4.ToString() + "";
                        //Console.WriteLine(query);



                        PointTableEnv = SQLServer.readDT(query);
                        //Console.WriteLine("PointTableEnv has " + PointTableEnv.Rows.Count + " records");
                        if (PointTableEnv.Rows.Count == 0)
                        {
                            Console.WriteLine("");
                            continue;
                        }

                        Console.Write("\t" + PointTableEnv.Rows.Count + "\tMessages in Envelope ");
                        PointTableEnv.Columns.Add("AMD3_ID", typeof(Int32));


                        //DataTable ResultTable = new DataTable();
                        //ResultTable.Columns.Add("msgID", typeof(Int64));
                        //ResultTable.Columns.Add("AMD3_ID", typeof(Int32));






                        SqlGeography polygon = new SqlGeography();
                        polygon = PolyTable.Rows[i].Field<SqlGeography>("Shape");



                        //Console.WriteLine(polygon);
                        SqlGeography point = new SqlGeography();
                        int IDXpoint = 1;
                        Console.WriteLine("");

                        //Parallel.ForEach(PointTableEnv.AsEnumerable(), new ParallelOptions { MaxDegreeOfParallelism = 3 }, dtRow =>
                        foreach (DataRow dtRow in PointTableEnv.Rows)
                        {
                            point = dtRow.Field<SqlGeography>("location");
                            Boolean inside = (Boolean)polygon.STIntersects(point);


                            //Console.WriteLine(dtRow.Field<Int64>("msgID"));
                            Console.Write("\r{0} Points checked  ", IDXpoint.ToString());


                            if (inside == true)
                            {
                                //Console.WriteLine("IDX: " + IDXpoint.ToString() + " " + point + " match? " + inside.ToString());
                                count = count + 1;
                                //query = "Update [weiboDEV].[dbo].[GEOminimal] Set [AMD3sing_OID]=" + PolyTable.Rows[i]["OBJECTID"].ToString() + " Where [idNearByTimeLine]=" + dtRow.Field<int>("idNearByTimeLine");
                                //SQLServer.updateTable(query);
                                dtRow.SetField<Int32>("AMD3_ID", polyID);

                                //DataRow row = ResultTable.NewRow();
                                //row["msgID"] = dtRow.Field<Int64>("msgID");
                                //row["AMD3_ID"] = polyID;
                                //ResultTable.Rows.Add(row);


                            }


                            IDXpoint++;




                        }//);


                        //

                        PointTableEnv.Columns.Remove("location");


                        //PointTableEnv.Columns.Add("AMD3_ID", typeof(Int32));



                        Console.Write(count.ToString() + " Points inside\t");


                        string destinationtable = "[dbo].[msgID_AMD3_ID]";
                        //SQLServer.writeDT(ResultTable, destinationtable);
                        SQLServer.WRITEDataTableToSQLServer(destinationtable, PointTableEnv, "weiboDEV");
                        Console.WriteLine(" now in " + destinationtable);

                        count = 0;
                        //Console.ReadLine();

                    }




                    Console.ReadLine();
                    break;
                case 2:
                    Console.WriteLine("Update Point with Hexagon ID");
                    Console.WriteLine(DateTime.Now + " Start to load HexagonTable with:");
                    query = "SELECT RandomID from weiboDEV.dbo.HEXAGONFIELDS_China";
                    Console.WriteLine(query);
                    DataTable HEXIDTable = new DataTable();
                    HEXIDTable = SQLServer.readDT(query);
                    //disp.dispDT(PolyTable);
                    Console.WriteLine("HEXIDTable has " + HEXIDTable.Rows.Count + " records");


                    int records = 1000;
                    int updated = 0;
                    for (Int32 i = 0; i < HEXIDTable.Rows.Count; i++)
                    {

                        string hexagon = HEXIDTable.Rows[i]["RandomID"].ToString();

                        string s = "Update top (" + records.ToString() + ") [weiboDEV].[dbo].GEOminimal SET [HEX_ID] = " + hexagon + " Where location.STIntersects((Select [Shape] FROM [weiboDEV].[dbo].[HEXAGONFIELDS_CHINA] WHERE RandomID = " + hexagon + ")) = 1 AND [HEX_ID] IS NULL;";
                        Console.WriteLine(s);
                        updated = SQLServer.updateTable(s);
                        Console.WriteLine("Updated " + updated + "for hexagon with RandomID: " + hexagon);
                        if (updated == records)
                        {
                            i = i - 1;
                        }
                        else
                        {
                            Console.Write(" moving on to next hexagon");
                        }


                    }

                    //
                    //    Console.WriteLine(HEXIDTable.Rows[i]["Input_FID"].ToString());
                    //}



                    //                    Update top (10000) [weiboDEV].[dbo].GEOminimal SET
                    //[HEX_ID] = 17881 
                    //Where location.STIntersects((Select [Shape] FROM [weiboDEV].[dbo].[HEXAGONFIELDS_CHINA] WHERE OBJECTID = 17881)) = 1
                    //AND 
                    //[HEX_ID] IS NULL;


                    Console.ReadLine();

                    break;
                case 3:
                    //take Points and find matching Polygon
                    Console.WriteLine("process 1: Update Points with ID from intersecting Polygon");
                    Console.WriteLine(DateTime.Now + " Start to load PolyTable with:");
                    query = "SELECT OBJECTID, Shape from weiboDEV.dbo.GADM_CHN_ADM3_SINGLE";
                    Console.WriteLine(query);
                    PolyTable = SQLServer.readDT(query);
                    //disp.dispDT(PolyTable);
                    Console.WriteLine("PolyTable has " + PolyTable.Rows.Count + " records");

                    Int64 msgIDmax = 0;
                    for (Int32 i = 0; i < 1000; i++)
                    {
                        Console.WriteLine("Select Points based on msgID");
                        if (i == 0)
                        {
                            query = "SELECT top 100 [msgID],[location] FROM weiboDEV.dbo.NBT4_exact_copy_GEO where [msgID] < 99999999999999999 AND [msgID] > " + msgIDmax.ToString() + " ORDER BY [msgID];";
                        }


                        PointTable = SQLServer.readDT(query);
                        //Console.WriteLine("PointTableEnv has " + PointTableEnv.Rows.Count + " records");
                        if (PointTable.Rows.Count == 0)
                        {
                            Console.WriteLine("");
                            continue;
                        }

                        //Add Column
                        PointTable.Columns.Add("AMD3_ID", typeof(Int32));
                        PointTable.AcceptChanges();


                        SqlGeography point = new SqlGeography();
                        SqlGeography polygon = new SqlGeography();
                        Int64 msgID = 0;
                        int IDX = 0;
                        //Parallel.ForEach(PointTable.AsEnumerable(), new ParallelOptions { MaxDegreeOfParallelism = 3 }, dtPOINTRow =>
                        foreach (DataRow dtPOINTRow in PointTable.Rows)
                        {
                            //Console.WriteLine(dtPOINTRow["msgID"].GetType() + dtPOINTRow["msgID"].ToString());

                            IDX++;
                            Console.WriteLine(IDX.ToString() + "\t");
                            point = dtPOINTRow.Field<SqlGeography>("location");
                            //find max msgID
                            msgID = (Int64)dtPOINTRow["msgID"];
                            if (msgID > msgIDmax)
                            {
                                msgIDmax = msgID;
                            }

                            //point.STIntersection(PointTable.Columns["Shape"]);


                            foreach (DataRow dtPOLYRow in PolyTable.Rows)
                            {
                                polygon = dtPOLYRow.Field<SqlGeography>("Shape");      //PolyTable.Rows[i].Field<SqlGeography>("Shape");

                                Boolean inside = (Boolean)polygon.STIntersects(point);
                                if (inside == false)
                                {
                                    dtPOINTRow.SetField<Int32>("AMD3_ID", (Int32)dtPOLYRow["OBJECTID"]); //set PolygonID in new table
                                    dtPOINTRow.AcceptChanges();
                                    continue;
                                }
                            }
                        }//);//end of parallel loop
                        disp.dispDT(PointTable);
                        Console.ReadLine();
                    }



                    break;

                case 4:
                    int[] finished = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 58, 61, 62, 63, 66, 76, 87, 88, 89, 90, 92, 94, 100, 101, 102, 103, 104, 105, 107, 120, 169, 170, 171, 172, 173, 174, 175, 177, 182, 185, 187, 188, 189, 222, 228, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262, 276, 277, 286, 287, 288, 289, 290, 291, 292, 293, 294, 295, 296, 297, 298, 299, 300, 301, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 320, 321, 322, 323, 324, 325, 326, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 352, 353, 354, 355, 356, 357, 358, 359, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395, 396, 397, 398, 399, 400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 419, 420, 421, 422, 423, 424, 425, 426, 427, 428, 429, 430, 431, 432, 433, 434, 435, 436, 437, 438, 439, 440, 441, 442, 443, 444, 445, 446, 447, 448, 449, 450, 457, 468, 469, 470, 471, 472, 473, 474, 475, 477, 478, 479, 481, 483, 487, 497, 499, 550, 579, 580, 581, 583, 584, 585, 586, 588, 590, 609, 624, 625, 630, 638, 639, 640, 641, 642, 643, 644, 645, 646, 647, 648, 649, 650, 654, 656, 659, 677, 679, 680, 684, 685, 689, 690, 693, 694, 696, 697, 698, 699, 700, 701, 702, 703, 704, 705, 706, 707, 708, 709, 710, 712, 714, 715, 716, 718, 720, 721, 722, 723, 725, 736, 743, 744, 745, 746, 757, 758, 759, 764, 767, 777, 786, 787, 788, 789, 790, 791, 792, 793, 794, 795, 796, 797, 798, 799, 800, 801, 802, 803, 804, 805, 806, 807, 808, 809, 810, 811, 812, 813, 814, 815, 816, 817, 818, 819, 820, 821, 822, 823, 824, 825, 826, 827, 828, 829, 830, 831, 832, 833, 834, 835, 836, 837, 838, 839, 840, 841, 842, 843, 844, 845, 846, 847, 848, 849, 850, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 865, 866, 882, 886, 887, 888, 889, 890, 891, 892, 893, 894, 895, 896, 897, 898, 899, 902, 903, 904, 905, 906, 907, 908, 909, 910, 911, 917, 920, 921, 924, 925, 927, 929, 930, 933, 934, 937, 938, 939, 940, 941, 942, 943, 944, 945, 946, 949, 971, 973, 974, 975, 977, 980, 981, 982, 983, 984, 985, 986, 994, 998, 999, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1020, 1021, 1023, 1026, 1029, 1033, 1034, 1036, 1041, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 2179, 2180, 2181, 2182, 2183, 2184, 2185, 2186, 2187, 2188, 2189, 2190, 2191, 2192, 2193, 2194, 2195, 2196, 2197, 2198, 2199, 2200, 2201, 2202, 2203, 2204, 2205, 2206, 2207, 2208, 2209, 2210, 2211, 2212, 2213, 2214, 2215, 2216, 2217, 2218, 2219, 2220, 2221, 2222, 2223, 2224, 2225, 2226, 2227, 2228, 2229, 2230, 2231, 2232, 2233, 2234, 3349, 3350, 3351, 3352, 3353, 3354, 3355, 3356, 3357, 3358, 3359, 3360, 3361, 3362, 3363, 3364, 3365, 3366, 3367, 3368, 3369, 3370, 3371, 3372, 3373, 3374, 3375, 3376, 3377, 3378, 3379, 3380, 3381, 3382, 3383 };


                    Console.WriteLine("At which ID (int) do you want to start?");
                    string line = Console.ReadLine();
                    int value;
                    if (int.TryParse(line, out value)) // Try to parse the string as an integer
                    {
                        Console.WriteLine("OK!");

                    }
                    else
                    {
                        Console.WriteLine("Not an integer!");
                    }

                    ParallelOptions options = new ParallelOptions();
                    options.MaxDegreeOfParallelism = 3;
                    

                    Parallel.For(value, 4521, options, i =>
                    //for (int i = value; i < 5000; i++)
                    {
                        int pos = Array.IndexOf(finished, i);
                        if (pos > -1)
                        {
                            Console.WriteLine("Call stored procedure test for Polygon ID:" + i.ToString() + " not necessary");
                        }
                        else
                        {
                            Console.WriteLine("Call stored procedure test for Polygon ID:" + i.ToString());
                        SQLServer.CallSQLStoredProcedure("weiboDEV", "test", i);
                        }
                        
                        




                    });


                    break;
                default:


                    //mytable.Columns.Add("geography", typeof(SqlGeography));
                    //mytable.Columns.Add("geometry", typeof(SqlGeometry));
                    //Console.WriteLine("");
                    //Console.WriteLine(mytable.Columns["location"].DataType);
                    //Console.WriteLine(mytable.Columns["geography"].DataType);
                    //Console.WriteLine(mytable.Columns["geometry"].DataType);

                    //DataTable geotable = new DataTable();
                    //geotable.Columns.Add("geography", typeof(SqlGeometry));

                    //Console.WriteLine("");
                    //disp.dispDT(mytable);





                    //DataTable shapesADM2 = new DataTable();
                    //Console.WriteLine(DateTime.Now + " Start to load table");
                    //string query2 = "select [OBJECTID],[Shape],[NAME_2] from weiboDEV.dbo.GADM_CHN_ADM2";
                    //Console.WriteLine(query2);
                    //shapesADM2 = SQLServer.readDT(query2);
                    ////disp.dispDT(shapesADM2); 
                    //Calculations.Intersect(mytable, shapesADM2);


                    Console.ReadLine();
                    break;
            }






        }
    }



    class disp
    {
        static public void dispDT(DataTable mytable)
        {
            //disp table on console
            if (Properties.Settings.Default.disp == true)
            {
                foreach (DataColumn column in mytable.Columns)
                {
                    Console.Write("{0,12}", column.ColumnName);
                    Console.Write(",");

                }

                foreach (DataRow row in mytable.Rows)
                {
                    Console.WriteLine();
                    for (int x = 0; x < mytable.Columns.Count; x++)
                    {
                        Console.Write("{0,12}", row[x].ToString() + " ");
                    }
                }

            }
        }

    }

    class SQLServer
    {
        static public SqlConnection GetSQLServerConnection(string db)
        {


            SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.SQL_Michael);

            //CONNECT
            if (db == "weiboDEV")
            {
                myConnection = new SqlConnection(Properties.Settings.Default.SQL_Michael);

            }
            if (db == "Weibo")
            {

                //myConnection = new SqlConnection(Properties.Settings.Default.MSSQLtimo);

            }
            //Console.WriteLine(myConnection.ToString());   


            try
            {
                myConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
            return myConnection;
        }

        static public int CallSQLStoredProcedure(string db, string proc, int id)
        {


            SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.SQL_Michael);

            //CONNECT
            if (db == "weiboDEV")
            {
                myConnection = new SqlConnection(Properties.Settings.Default.SQL_Michael);

            }
            if (db == "Weibo")
            {

                //myConnection = new SqlConnection(Properties.Settings.Default.MSSQLtimo);

            }
            //Console.WriteLine(myConnection.ToString());   
            int result = 0;

            try
            {
                myConnection.Open();


                SqlCommand comm = myConnection.CreateCommand();
                comm.CommandType = CommandType.StoredProcedure;
                comm.CommandText = proc;
                comm.CommandTimeout = 10000; //5 mins
                comm.Parameters.Add(new SqlParameter("@ID", id.ToString()));
                comm.ExecuteReader(System.Data.CommandBehavior.CloseConnection);



            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
            myConnection.Close();
            return result;
        }

        static public DataTable readDT(string query)
        {
            //CONNECT

            SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.SQL_Michael);
            //myConnection.ChangeDatabase("weiboDEV");

            //Console.WriteLine(myConnection.ConnectionTimeout.ToString());
            try
            {
                myConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

            SqlCommand command = new SqlCommand(query, myConnection);
            command.CommandTimeout = 3600; //5 mins

            var table = new DataTable();
            //using (var da = new SqlDataAdapter(query, myConnection))
            using (var da = new SqlDataAdapter(command))
            {

                da.Fill(table);
            }

            myConnection.Close();

            return table;


        }

        static public void writeDT(DataTable dt, string tablename)
        {
            //SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(Properties.Settings.Default.SQL_Michael);
            //sqlBulkCopy.DestinationTableName = "MySpatialDataTable";
            //sqlBulkCopy.WriteToServer(dataTable);

            string connectionString = Properties.Settings.Default.SQL_Michael;
            // Open a connection to the AdventureWorks database. 
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();




                // Create the SqlBulkCopy object.  
                // Note that the column positions in the source DataTable  
                // match the column positions in the destination table so  
                // there is no need to map columns.  
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = tablename;

                    try
                    {
                        // Write from the source to the destination.
                        bulkCopy.WriteToServer(dt);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }

                connection.Close();

            }

        }

        static public bool WRITEDataTableToSQLServer(string tableName, DataTable dataTable, string database)
        {

            bool isSuccuss;
            using (TransactionScope scope = new TransactionScope())
            {

                try
                {
                    SqlConnection SqlConnectionObj = SQLServer.GetSQLServerConnection(database);

                    SqlBulkCopy bulkCopy = new SqlBulkCopy(SqlConnectionObj, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.UseInternalTransaction, null);
                    bulkCopy.BulkCopyTimeout = 3600;
                    bulkCopy.DestinationTableName = tableName;
                    bulkCopy.WriteToServer(dataTable);
                    isSuccuss = true;
                }
                catch (Exception ex)
                {
                    isSuccuss = false;
                    Console.WriteLine(ex.ToString());
                }
                scope.Complete();
            }


            return isSuccuss;

        }

        static public int updateTable(string statement)
        {

            int i = 1;

            //CONNECT

            //SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.SQL_Michael);
            //myConnection.ChangeDatabase("weiboDEV");

            using (SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.SQL_Michael))

            using (SqlCommand command = myConnection.CreateCommand())
            {
                command.CommandText = statement;

                try
                {

                    myConnection.Open();
                    command.CommandTimeout = 3600;
                    i = command.ExecuteNonQuery();



                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                }
                myConnection.Close();

            }






            return i;
        }
    }

    class Calculations
    {
        static public void Intersect(DataTable points, DataTable polygons)
        {
            foreach (DataRow row in points.Rows)
            {
                Console.WriteLine(row["location"]);
                //SqlGeography g1 = (SqlGeography)row["location"];
                SqlGeography g1 = SqlGeography.Parse(row["location"].ToString());

                foreach (DataRow rowS in polygons.Rows)
                {
                    //Console.WriteLine("{0,12}", rowS["OBJECTID"].ToString() + " ");
                    //row["location"].STIntersects(rowS["shape"]);

                    //SqlGeometry g1 = SqlGeography.Point;
                    SqlGeography g2 = SqlGeography.Parse(rowS["shape"].ToString());
                    SqlGeography intersection = g1.STIntersection(g2);
                    //Console.WriteLine(intersection.ToString());


                }

            }



        }

        static public Tuple<double, double, double, double> GetMaxMinXYFromPolygonstring(string poly, double buf)
        {


            string[] coordinates = poly.Split(' ');
            string latORlon = "lon";
            double minX = 9999.99;
            double minY = 9999.99;
            double maxX = -9999.99;
            double maxY = -9999.99;
            int index = 0;
            string Snumber;
            double number = 0.0;
            foreach (string coordinate in coordinates)
            {
                if (index < 0)             //                                |
                {                           //                                |
                    continue;   // Skip the remainder of this iteration. -----+
                }
                Snumber = Regex.Replace(coordinate, @"[ABCDEFGHIJKLMNOPQRSTUVWXYZ(),\n\r ]", String.Empty);
                if (string.IsNullOrEmpty(Snumber) == true)
                {
                    // first part of text
                    //Console.WriteLine("damn!"); 
                    continue;
                }
                else
                {
                    number = Convert.ToDouble(Snumber);

                }

                //Console.WriteLine(number.ToString());
                if (latORlon == "lon")
                {
                    //get max
                    if (number > maxX)
                    {
                        maxX = number;
                    }
                    //get min
                    if (number < minX)
                    {
                        minX = number;
                    }

                    latORlon = "lat";
                }
                else
                {
                    //get max
                    if (number > maxY)
                    {
                        maxY = number;
                    }
                    //get min
                    if (number < minY)
                    {
                        minY = number;
                    }
                    latORlon = "lon";
                }
            }

            // Console.WriteLine(" minX: " + minX.ToString() + " maxX: " + maxX.ToString() + " minY: " + minY.ToString() + " smaxY: " + maxY.ToString());

            return Tuple.Create(minX - buf, maxX + buf, minY - buf, maxY + buf);
        }
    }
}

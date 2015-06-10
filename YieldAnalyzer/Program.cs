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
            int process = 1;

            switch (process)
            {
                case 1: //Intersecting points with Polygon
                    Console.WriteLine("process 1: Update Points with ID from intersecting Polygon");
                    Console.WriteLine(DateTime.Now + " Start to load PolyTable with:");
                    string query = "SELECT OBJECTID, Shape from weiboDEV.dbo.GADM_CHN_ADM3_SINGLE";
                    //Console.WriteLine(query);
                    DataTable PolyTable = new DataTable();
                    PolyTable = SQLServer.readDT(query);
                    //disp.dispDT(PolyTable);
                    Console.WriteLine("PolyTable has " + PolyTable.Rows.Count + " records");


                    int count = 0;
                    for (Int32 i = 406; i < PolyTable.Rows.Count; i++)
                    {
                        string poly = PolyTable.Rows[i]["Shape"].ToString();
                        //Console.WriteLine(PolyTable.Rows[i]["OBJECTID"].GetType());
                        Int32 polyID = (Int32)PolyTable.Rows[i]["OBJECTID"];


                        Console.Write("At polyID: "+ polyID.ToString() + "\t");


                        double buffer = 0.0001;
                        var dims = Calculations.GetMaxMinXYFromPolygonstring(poly, buffer);
                        //Console.Write(PolyTable.Rows[i]["OBJECTID"].ToString() + " ");
                        //Console.WriteLine(dims);


                        query = "Select [msgID],[location] from [weiboDEV].[dbo].[GEOminimal] WHERE [WGSLongitudeY] > " + dims.Item1.ToString() + " AND [WGSLongitudeY] < " + dims.Item2.ToString() + " AND [WGSLatitudeX] > " + dims.Item3.ToString() + " AND [WGSLatitudeX] < " + dims.Item4.ToString() + "";
                        //Console.WriteLine(query);
                        
                        
                        DataTable PointTableEnv = new DataTable();
                        PointTableEnv = SQLServer.readDT(query);
                        //Console.WriteLine("PointTableEnv has " + PointTableEnv.Rows.Count + " records");
                        if (PointTableEnv.Rows.Count==0) {
                            Console.WriteLine("");
                            continue;
                        }

                        Console.Write("\t" + PointTableEnv.Rows.Count + "\tMessages in Envelope ");



                        DataTable ResultTable = new DataTable();
                        ResultTable.Columns.Add("msgID", typeof(Int64));
                        ResultTable.Columns.Add("AMD3_ID", typeof(Int32));






                        SqlGeography polygon = new SqlGeography();
                        polygon = PolyTable.Rows[i].Field<SqlGeography>("Shape");

                        

                        //Console.WriteLine(polygon);
                        SqlGeography point = new SqlGeography();
                        int IDXpoint = 1;
                        Console.WriteLine("");

                        //Parallel.ForEach(PointTableEnv.AsEnumerable(), new ParallelOptions { MaxDegreeOfParallelism = 3 }, dtRow =>
                        foreach(DataRow dtRow in PointTableEnv.Rows)
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


                                DataRow row = ResultTable.NewRow();
                                row["msgID"] = dtRow.Field<Int64>("msgID");
                                row["AMD3_ID"] = polyID;
                                ResultTable.Rows.Add(row);


                            }
                            //else {
                            //    dtRow.Delete();

                            //}

                            IDXpoint++;




                        }//);
                        
                        
                        //disp.dispDT(ResultTable);



                       
                        Console.Write(count.ToString() + " Points inside\t");


                        string destinationtable = "[dbo].[msgID_AMD3_ID]";
                        //SQLServer.writeDT(ResultTable, destinationtable);
                        SQLServer.WRITEDataTableToSQLServer(destinationtable, ResultTable, "weiboDEV");
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

        static public void writeDT(DataTable dt,string tablename)
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

                    SqlBulkCopy bulkCopy = new SqlBulkCopy(SqlConnectionObj, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.UseInternalTransaction, null );
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

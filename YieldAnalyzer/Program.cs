using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using Microsoft.SqlServer.Types;
using System.Text.RegularExpressions;
//using Calculations;
using ProSQLSpatial;

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
                    string query = "SELECT ID_3, Shape from weiboDEV.dbo.GADM_CHN_ADM3_SINGLE";
                    Console.WriteLine(query);
                    DataTable PolyTable = new DataTable();
                    PolyTable = SQLServer.readDT(query);
                    //disp.dispDT(PolyTable);
                    Console.WriteLine("PolyTable has " + PolyTable.Rows.Count + " records");
                    
                    for (Int32 i = 0; i < PolyTable.Rows.Count; i++)
                    {
                        string poly = PolyTable.Rows[i]["Shape"].ToString();
                       
                        string[] coordinates = poly.Split(' ');
                        string latORlon = "lon";
                        double minX = 9999.99;
                        double minY = 9999.99;
                        double maxX = -9999.99;
                        double maxY = -9999.99;
                        int index = 0;
                        string Snumber;
                        double number=0.0;
                        foreach (string coordinate in coordinates)
                        {
                            if (index < 0)             //                                |
                            {                           //                                |
                                continue;   // Skip the remainder of this iteration. -----+
                            }
                            Snumber = Regex.Replace(coordinate, @"[ABCDEFGHIJKLMNOPQRSTUVWXYZ(),\n\r ]", String.Empty);
                            if (string.IsNullOrEmpty(Snumber) == true) {
                                // first part of text
                                //Console.WriteLine("damn!"); 
                                continue;
                            }
                            else
                            {
                                number = Convert.ToDouble(Snumber);

                            }

                            //Console.WriteLine(number.ToString());
                            if (latORlon=="lon"){
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
                            }else{
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
                                latORlon="lon";
                            }
                        }

                        Console.WriteLine("ID_3: " + PolyTable.Rows[i]["ID_3"].ToString() + " minX: " + minX.ToString() + " maxX: " + maxX.ToString() + " minY: " + minY.ToString() + " smaxY: " + maxY.ToString());

                        
                    }
Console.ReadLine();

                    
                    for (Int32 i = 0; i < PolyTable.Rows.Count; i++)
                    {
                        string ShapeID = PolyTable.Rows[i]["ID_3"].ToString();
                        query = "Select geography::STPolyFromText(geometry::UnionAggregate ( geometry::STGeomFromText(cast([Shape] as varchar(max)), 4326)  ).STEnvelope().STAsText(),4326).STPointN(1).Lat AS minY, geography::STPolyFromText(geometry::UnionAggregate ( geometry::STGeomFromText(cast([Shape] as varchar(max)), 4326)  ).STEnvelope().STAsText(),4326).STPointN(1).Long AS minX,geography::STPolyFromText(geometry::UnionAggregate ( geometry::STGeomFromText(cast([Shape] as varchar(max)), 4326)  ).STEnvelope().STAsText(),4326).STPointN(3).Lat AS maxY,geography::STPolyFromText(geometry::UnionAggregate ( geometry::STGeomFromText(cast([Shape] as varchar(max)), 4326)  ).STEnvelope().STAsText(),4326).STPointN(3).Long AS maxX FROM [weiboDEV].[dbo].[GADM_CHN_ADM2] WHERE OBJECTID = " + ShapeID + ";";
                        DataTable dimensionsRectangle = new DataTable();
                        dimensionsRectangle = SQLServer.readDT(query);

                        Console.WriteLine("IDX: " + (i).ToString() + "  " +
                            " minY: " + dimensionsRectangle.Rows[0]["minY"].ToString() + " " +
                            " minX: " + dimensionsRectangle.Rows[0]["minX"].ToString() + " " +
                            " maxY: " + dimensionsRectangle.Rows[0]["maxY"].ToString() + " " +
                            " maxX: " + dimensionsRectangle.Rows[0]["maxX"].ToString()
                                            );
                    }

                    Console.ReadLine();

                    Console.WriteLine(DateTime.Now + " Start to load Points with:");
                    string query2 = "select top 10000 [idNearByTimeLine],[location] from weiboDEV.dbo.GEOminimal;";
                    Console.WriteLine(query2);
                    DataTable PointTable = new DataTable();
                    PointTable = SQLServer.readDT(query2);
                    //disp.dispDT(PolyTable);
                    Console.WriteLine("PolyTable has " + PointTable.Rows.Count + " records");

                    var h = Microsoft.SqlServer.Types.SqlGeography.Point(47.653d, -122.358d, 4326);
                    Console.WriteLine(PointTable.Rows[1]["location"].GetType());

                    Console.WriteLine(PointTable.Rows[1]["location"].ToString());








                    for (Int32 i = 0; i < PointTable.Rows.Count; i++)
                    {
                        Console.WriteLine(PolyTable.Rows[i]["location"].ToString());

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
        static public DataTable readDT(string query)
        {
            //CONNECT

            SqlConnection myConnection = new SqlConnection(Properties.Settings.Default.SQL_Michael);
            //myConnection.ChangeDatabase("weiboDEV");

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

        static public void writeDT(DataTable dataTable)
        {
            SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(Properties.Settings.Default.SQL_Michael);
            sqlBulkCopy.DestinationTableName = "MySpatialDataTable";
            sqlBulkCopy.WriteToServer(dataTable);
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
    }
}

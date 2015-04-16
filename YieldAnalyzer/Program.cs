using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using Microsoft.SqlServer.Types;

//using Calculations;
using ProSQLSpatial;


namespace YieldAnalyzer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine(DateTime.Now + " 你好 Welcome");
           
            DataTable mytable = new DataTable();
            Console.WriteLine(DateTime.Now + " Start to load table");
            string query = "select top 1 * from weiboDEV.dbo.GEOminimal";
            Console.WriteLine(query);
            mytable = SQLServer.readDT(query);
            disp.dispDT(mytable);

            mytable.Columns.Add("geography", typeof(SqlGeography));
            mytable.Columns.Add("geometry", typeof(SqlGeometry));
            Console.WriteLine("");
            Console.WriteLine(mytable.Columns["location"].DataType);
            Console.WriteLine(mytable.Columns["geography"].DataType);
            Console.WriteLine(mytable.Columns["geometry"].DataType);
            
            DataTable geotable = new DataTable();
            geotable.Columns.Add("geography", typeof(SqlGeometry));
            
            Console.WriteLine("");
            disp.dispDT(mytable);





            DataTable shapesADM2 = new DataTable();
            Console.WriteLine(DateTime.Now + " Start to load table");
            string query2 = "select [OBJECTID],[Shape],[NAME_2] from weiboDEV.dbo.GADM_CHN_ADM2";
            Console.WriteLine(query2);
            shapesADM2 = SQLServer.readDT(query2);
            //disp.dispDT(shapesADM2); 
            Calculations.Intersect(mytable, shapesADM2);


            Console.ReadLine();

        }
    }



    class disp { 
    static public void dispDT(DataTable mytable){
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

    class SQLServer {
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
    }

    class Calculations {
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

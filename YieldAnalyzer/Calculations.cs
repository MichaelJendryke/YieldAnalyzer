using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Types;

namespace Calculations
{
           class triangulate
        {

            private struct SimplePoint : IComparable
            {
                public double x, y;
                //Constructor
                public SimplePoint(double x, double y)
                {
                    this.x = x;
                    this.y = y;
                }
                //Implement IComparable CompareTo method to enable sorting
                int IComparable.CompareTo(object obj)
                {
                    SimplePoint other = (SimplePoint)obj;
                    if (this.x > other.x)
                    {
                        return 1;
                    }
                    else if (this.x < other.x)
                    {
                        return -1;
                    }
                    else
                    {
                        return 0;
                    }
                }
            }

            private struct SimpleTriangle
            {
                //Index entries to each vertex
                public int a, b, c;

                //Circumcenter and radius od circumcircle
                public SimplePoint circumcentre;
                public double radius;

                //Constructor
                public SimpleTriangle(int a, int b, int c, SimplePoint circumcentre, double radius)
                {
                    this.a = a;
                    this.b = b;
                    this.c = c;
                    this.circumcentre = circumcentre;
                    this.radius = radius;
                }
            }

            private static void CalculateCircumcircle(SimplePoint p1,
                                                      SimplePoint p2,
                                                      SimplePoint p3,
                                                      out SimplePoint circumCentre,
                                                      out double radius)
            {
                // Calculate the length of each side of the triangle
                double a = Distance(p2, p3);
                double b = Distance(p1, p3);
                double c = Distance(p1, p2);

                // Calculate the radius of the circumcircle
                double area = Math.Abs((double)(p1.x * (p2.y - p3.y) + p2.x * (p3.y - p1.y) + p3.x * (p1.y - p2.y)) / 2);
                radius = a * b * c / (4 * area);

                //Define the area coordinates to calculate the circumcentre
                double pp1 = Math.Pow(a, 2) * (Math.Pow(b, 2) + Math.Pow(c, 2) - Math.Pow(a, 2));
                double pp2 = Math.Pow(b, 2) * (Math.Pow(c, 2) + Math.Pow(a, 2) - Math.Pow(b, 2));
                double pp3 = Math.Pow(c, 2) * (Math.Pow(a, 2) + Math.Pow(b, 2) - Math.Pow(c, 2));

                //Normalize
                double t1 = pp1 / (pp1 + pp2 + pp3);
                double t2 = pp2 / (pp1 + pp2 + pp3);
                double t3 = pp3 / (pp1 + pp2 + pp3);

                //Convert to Cartesian
                double x = t1 * p1.x + t2 * p2.x + t3 * p3.x;
                double y = t1 * p1.y + t2 * p2.y + t3 * p3.y;

                //Define the circumcenter
                circumCentre = new SimplePoint(x, y);
            }

            // BECAREFULL this is only on a flat plane
            private static double Distance(SimplePoint p1, SimplePoint p2)
            {
                double result = 0;
                result = Math.Sqrt(Math.Pow((p2.x - p1.x), 2) + Math.Pow((p2.y - p1.y), 2));
                return result;
            }

            [Microsoft.SqlServer.Server.SqlProcedure]
            public static void GeometryTriangulate(SqlGeometry MultiPoint)
            {



                List<SimplePoint> Vertices = new List<SimplePoint>();
                // Loop through supplied points
                for (int i = 1; i <= MultiPoint.STNumPoints(); i++)
                {
                    //Create a new simple point corresponding to this element of the MultiPoint
                    SimplePoint Point = new SimplePoint(
                        (double)MultiPoint.STPointN(i).STX,
                        (double)MultiPoint.STPointN(i).STY
                        );
                    //Check whether this this point already exists
                    if (!Vertices.Contains(Point))
                    {
                        Vertices.Add(Point);
                    }
                }
                Vertices.Sort();

                //Creating the Supertriangle
                SqlGeometry Envelope = MultiPoint.STEnvelope();
                //width
                double dx = (double)(Envelope.STPointN(2).STX - Envelope.STPointN(1).STX);
                //height
                double dy = (double)(Envelope.STPointN(4).STY - Envelope.STPointN(1).STY);
                // max dimension
                double dmax = (dx > dy) ? dx : dy;

                //Centre
                SqlGeometry centroid = Envelope.STCentroid();
                double avgx = (double)centroid.STX;
                double avgy = (double)centroid.STY;

                SimplePoint a = new SimplePoint(avgx - 2 * dmax, avgy - dmax);
                SimplePoint b = new SimplePoint(avgx + 2 * dmax, avgy - dmax);
                SimplePoint c = new SimplePoint(avgx, avgy + 2 * dmax);

                //Add the supertriangle to the end of the vertex array
                Vertices.Add(a);
                Vertices.Add(b);
                Vertices.Add(c);

                //create the supertringle
                double radius;
                SimplePoint circumcentre;
                CalculateCircumcircle(a, b, c, out circumcentre, out radius);
                /////SimpleTriangle SuperTriangle = new SimpleTriangle(numPoints, numPoints + 1, numPoints + 2, circumcentre, radius);

                //Add the supertriangle
                List<SimpleTriangle> Triangles = new List<SimpleTriangle>();
                //////Triangles.Add(SimpleTriangle);

                //Create an empty list to hold completed triangles
                List<SimpleTriangle> CompletedTriangle = new List<SimpleTriangle>();



            }
        }
    
 
}

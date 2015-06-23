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
                    int[] finished = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 58, 61, 62, 63, 66, 76, 87, 88, 89, 90, 92, 94, 100, 101, 102, 103, 104, 105, 107, 120, 169, 170, 171, 172, 173, 174, 175, 177, 182, 185, 187, 188, 189, 222, 228, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262, 276, 277, 286, 287, 288, 289, 290, 291, 292, 293, 294, 295, 296, 297, 298, 299, 300, 301, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 320, 321, 322, 323, 324, 325, 326, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 342, 343, 344, 345, 346, 347, 348, 349, 350, 351, 352, 353, 354, 355, 356, 357, 358, 359, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395, 396, 397, 398, 399, 400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 419, 420, 421, 422, 423, 424, 425, 426, 427, 428, 429, 430, 431, 432, 433, 434, 435, 436, 437, 438, 439, 440, 441, 442, 443, 444, 445, 446, 447, 448, 449, 450, 457, 468, 469, 470, 471, 472, 473, 474, 475, 476, 477, 478, 479, 481, 483, 487, 495, 497, 499, 550, 579, 580, 581, 583, 584, 585, 586, 588, 590, 609, 624, 625, 630, 638, 639, 640, 641, 642, 643, 644, 645, 646, 647, 648, 649, 650, 654, 656, 659, 677, 679, 680, 684, 685, 689, 690, 693, 694, 696, 697, 698, 699, 700, 701, 702, 703, 704, 705, 706, 707, 708, 709, 710, 712, 714, 715, 716, 718, 720, 721, 722, 723, 725, 736, 743, 744, 745, 746, 757, 758, 759, 764, 767, 768, 777, 786, 787, 788, 789, 790, 791, 792, 793, 794, 795, 796, 797, 798, 799, 800, 801, 802, 803, 804, 805, 806, 807, 808, 809, 810, 811, 812, 813, 814, 815, 816, 817, 818, 819, 820, 821, 822, 823, 824, 825, 826, 827, 828, 829, 830, 831, 832, 833, 834, 835, 836, 837, 838, 839, 840, 841, 842, 843, 844, 845, 846, 847, 848, 849, 850, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 865, 866, 882, 886, 887, 888, 889, 890, 891, 892, 893, 894, 895, 896, 897, 898, 899, 902, 903, 904, 905, 906, 907, 908, 909, 910, 911, 917, 920, 921, 924, 925, 927, 929, 930, 933, 934, 937, 938, 939, 940, 941, 942, 943, 944, 945, 946, 949, 971, 973, 974, 975, 977, 980, 981, 982, 983, 984, 985, 986, 994, 998, 999, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1020, 1021, 1023, 1026, 1029, 1033, 1034, 1036, 1041, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1051, 1052, 1053, 1054, 1055, 1056, 1057, 1077, 1078, 1080, 1081, 1082, 1083, 1084, 1085, 1086, 1089, 1094, 1095, 1098, 1108, 1109, 1110, 1122, 1131, 1132, 1133, 1134, 1135, 1136, 1138, 1141, 1142, 1143, 1144, 1152, 1153, 1154, 1155, 1157, 1160, 1162, 1164, 1165, 1172, 1177, 1178, 1179, 1180, 1181, 1182, 1183, 1185, 1186, 1187, 1190, 1193, 1203, 1206, 1207, 1208, 1210, 1212, 1213, 1216, 1217, 1221, 1229, 1234, 1240, 1257, 1263, 1268, 1269, 1270, 1271, 1272, 1273, 1274, 1275, 1276, 1277, 1278, 1279, 1280, 1281, 1283, 1290, 1291, 1292, 1293, 1294, 1295, 1296, 1297, 1301, 1303, 1308, 1318, 1330, 1331, 1332, 1333, 1334, 1335, 1336, 1337, 1338, 1339, 1340, 1341, 1342, 1343, 1344, 1345, 1346, 1347, 1348, 1349, 1350, 1351, 1352, 1353, 1354, 1355, 1356, 1357, 1358, 1359, 1360, 1361, 1362, 1363, 1364, 1392, 1395, 1396, 1397, 1398, 1399, 1400, 1401, 1402, 1403, 1404, 1405, 1406, 1407, 1408, 1409, 1410, 1411, 1412, 1413, 1414, 1415, 1416, 1417, 1418, 1419, 1420, 1421, 1422, 1423, 1424, 1425, 1426, 1427, 1428, 1429, 1430, 1431, 1432, 1433, 1434, 1435, 1436, 1437, 1438, 1439, 1440, 1441, 1442, 1443, 1444, 1446, 1447, 1448, 1449, 1450, 1451, 1452, 1453, 1454, 1455, 1456, 1457, 1458, 1459, 1460, 1461, 1462, 1463, 1464, 1465, 1466, 1467, 1468, 1469, 1470, 1471, 1472, 1473, 1474, 1475, 1476, 1477, 1478, 1479, 1480, 1481, 1482, 1483, 1484, 1485, 1486, 1487, 1488, 1489, 1490, 1491, 1492, 1498, 1499, 1500, 1501, 1506, 1507, 1508, 1511, 1512, 1513, 1514, 1516, 1517, 1519, 1520, 1521, 1522, 1523, 1524, 1525, 1526, 1527, 1528, 1531, 1534, 1539, 1540, 1541, 1550, 1551, 1552, 1553, 1554, 1555, 1556, 1557, 1558, 1559, 1560, 1561, 1562, 1563, 1564, 1565, 1566, 1567, 1568, 1569, 1570, 1571, 1572, 1573, 1574, 1575, 1576, 1577, 1578, 1579, 1580, 1581, 1582, 1583, 1584, 1585, 1586, 1587, 1588, 1589, 1590, 1591, 1592, 1593, 1594, 1595, 1596, 1597, 1598, 1599, 1600, 1601, 1602, 1603, 1604, 1605, 1606, 1607, 1608, 1609, 1610, 1611, 1612, 1613, 1615, 1617, 1618, 1619, 1620, 1621, 1622, 1623, 1624, 1625, 1626, 1627, 1628, 1629, 1630, 1631, 1632, 1633, 1634, 1636, 1637, 1638, 1646, 1647, 1648, 1649, 1650, 1651, 1652, 1653, 1654, 1656, 1657, 1658, 1659, 1660, 1661, 1662, 1663, 1664, 1665, 1666, 1667, 1668, 1669, 1670, 1671, 1672, 1673, 1674, 1675, 1676, 1677, 1678, 1679, 1680, 1681, 1682, 1683, 1684, 1685, 1686, 1689, 1690, 1691, 1700, 1701, 1702, 1703, 1713, 1728, 1729, 1730, 1735, 1736, 1737, 1738, 1739, 1740, 1741, 1742, 1743, 1744, 1745, 1746, 1747, 1748, 1749, 1750, 1751, 1752, 1753, 1754, 1755, 1756, 1757, 1758, 1759, 1760, 1761, 1762, 1763, 1764, 1765, 1766, 1767, 1768, 1769, 1770, 1771, 1772, 1773, 1774, 1775, 1776, 1777, 1778, 1779, 1780, 1781, 1782, 1783, 1784, 1785, 1786, 1788, 1789, 1790, 1791, 1792, 1793, 1794, 1795, 1796, 1797, 1798, 1799, 1802, 1803, 1804, 1805, 1806, 1807, 1808, 1809, 1810, 1811, 1812, 1813, 1814, 1815, 1816, 1817, 1818, 1819, 1820, 1821, 1822, 1823, 1824, 1825, 1826, 1827, 1828, 1829, 1830, 1831, 1832, 1833, 1834, 1835, 1836, 1837, 1838, 1839, 1840, 1841, 1842, 1843, 1844, 1845, 1846, 1847, 1848, 1849, 1850, 1851, 1852, 1853, 1854, 1855, 1856, 1858, 1859, 1860, 1861, 1862, 1863, 1864, 1865, 1866, 1867, 1868, 1869, 1870, 1871, 1872, 1873, 1874, 1875, 1876, 1877, 1878, 1879, 1880, 1881, 1882, 1883, 1884, 1885, 1886, 1887, 1888, 1889, 1890, 1891, 1892, 1893, 1894, 1895, 1896, 1897, 1898, 1899, 1900, 1901, 1902, 1903, 1904, 1905, 1906, 1907, 1908, 1909, 1910, 1911, 1912, 1913, 1914, 1915, 1916, 1917, 1918, 1919, 1920, 1921, 1922, 1923, 1924, 1925, 1926, 1927, 1928, 1929, 1930, 1931, 1932, 1933, 1934, 1935, 1936, 1937, 1938, 1939, 1940, 1941, 1942, 1943, 1944, 1945, 1946, 1947, 1948, 1949, 1950, 1951, 1952, 1953, 1954, 1955, 1956, 1957, 1958, 1959, 1960, 1961, 1962, 1963, 1964, 1965, 1966, 1967, 1968, 1969, 1970, 1971, 1972, 1973, 1974, 1975, 1976, 1977, 1978, 1979, 1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026, 2027, 2179, 2180, 2181, 2182, 2183, 2184, 2185, 2186, 2187, 2188, 2189, 2190, 2191, 2192, 2193, 2194, 2195, 2196, 2197, 2198, 2199, 2200, 2201, 2202, 2203, 2204, 2205, 2206, 2207, 2208, 2209, 2210, 2211, 2212, 2213, 2214, 2215, 2216, 2217, 2218, 2219, 2220, 2221, 2222, 2223, 2224, 2225, 2226, 2227, 2228, 2229, 2230, 2231, 2232, 2233, 2234, 3019, 3021, 3023, 3025, 3026, 3027, 3028, 3029, 3030, 3031, 3032, 3033, 3034, 3035, 3036, 3040, 3045, 3046, 3050, 3059, 3060, 3076, 3091, 3110, 3111, 3112, 3113, 3114, 3115, 3116, 3117, 3118, 3119, 3120, 3121, 3122, 3123, 3124, 3125, 3126, 3127, 3128, 3129, 3130, 3131, 3132, 3133, 3134, 3135, 3136, 3137, 3138, 3139, 3140, 3141, 3142, 3144, 3145, 3146, 3147, 3148, 3149, 3150, 3151, 3152, 3153, 3154, 3155, 3156, 3157, 3158, 3159, 3160, 3161, 3162, 3163, 3164, 3165, 3166, 3167, 3168, 3169, 3170, 3171, 3172, 3173, 3174, 3175, 3176, 3177, 3178, 3179, 3180, 3181, 3182, 3183, 3184, 3186, 3187, 3188, 3189, 3190, 3191, 3192, 3193, 3194, 3195, 3196, 3197, 3198, 3199, 3200, 3201, 3202, 3203, 3204, 3205, 3206, 3207, 3208, 3209, 3210, 3211, 3212, 3213, 3214, 3215, 3216, 3217, 3218, 3219, 3220, 3222, 3223, 3224, 3225, 3226, 3227, 3228, 3229, 3230, 3231, 3232, 3233, 3234, 3235, 3236, 3237, 3238, 3239, 3240, 3241, 3242, 3243, 3244, 3245, 3246, 3247, 3248, 3249, 3250, 3251, 3252, 3253, 3254, 3255, 3256, 3257, 3258, 3259, 3260, 3261, 3262, 3263, 3264, 3265, 3266, 3267, 3268, 3269, 3270, 3271, 3272, 3273, 3274, 3275, 3276, 3277, 3278, 3279, 3280, 3281, 3282, 3283, 3284, 3285, 3286, 3287, 3288, 3289, 3290, 3291, 3292, 3293, 3294, 3295, 3296, 3297, 3298, 3299, 3300, 3301, 3302, 3303, 3304, 3305, 3306, 3307, 3308, 3309, 3310, 3311, 3312, 3313, 3314, 3315, 3316, 3317, 3318, 3319, 3320, 3321, 3322, 3323, 3324, 3325, 3326, 3327, 3328, 3329, 3330, 3331, 3332, 3333, 3334, 3335, 3336, 3337, 3338, 3339, 3340, 3341, 3342, 3343, 3344, 3345, 3346, 3347, 3348, 3349, 3350, 3351, 3352, 3353, 3354, 3355, 3356, 3357, 3358, 3359, 3360, 3361, 3362, 3363, 3364, 3365, 3366, 3367, 3368, 3369, 3370, 3371, 3372, 3373, 3374, 3375, 3376, 3377, 3378, 3379, 3380, 3381, 3382, 3383, 3384, 3385, 3386, 3387, 3388, 3389, 3390, 3391, 3393, 3394, 3395, 3396, 3397, 3398, 3399, 3400, 3401, 3402, 3404, 3405, 3406, 3407, 3408, 3409, 3410, 3411, 3412, 3413, 3414, 3415, 3416, 3417, 3418, 3419, 3420, 3421, 3422, 3423, 3424, 3425, 3426, 3427, 3428, 3429, 3430, 3431, 3432, 3433, 3434, 3435, 3436, 3443, 3444, 3445, 3446, 3447, 3448, 3449, 3450, 3451, 3452, 3453, 3454 };

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
                    String StoredProcedure = "SelectPointsByPolyIDIntoTable";

                    Parallel.For(value, 4521, options, i =>
                    //for (int i = value; i < 5000; i++)
                    {
                        int pos = Array.IndexOf(finished, i);
                        if (pos > -1)
                        {
                            Console.WriteLine("Call stored procedure " + StoredProcedure + " for Polygon ID:" + i.ToString() + " not necessary");
                        }
                        else
                        {
                            Console.WriteLine("Call stored procedure " + StoredProcedure + " for Polygon ID:" + i.ToString());
                            SQLServer.CallSQLStoredProcedure("weiboDEV", StoredProcedure, i);
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

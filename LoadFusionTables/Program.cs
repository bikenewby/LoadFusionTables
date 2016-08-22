using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Threading;

namespace LoadFusionTables
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                PokestopsDB db = new PokestopsDB();
                PokestopsCloudDB cloudDB = new PokestopsCloudDB();
                cloudDB.openDB().Wait();
                Console.WriteLine("Cloud DB Opened.");
                Console.ReadKey();
                db.openDB();

                Console.WriteLine("DB Connection Opened");
                DataSet ds = db.loadTable();
                Console.WriteLine("DB Loaded");
                DataRowCollection dra = ds.Tables["Pokestops"].Rows;
                int totalRead = dra.Count;
                int insertCount = 0;
                foreach (DataRow dr in dra)
                {
                    // Print the CategoryID as a subscript, then the CategoryName:
                    int result = 0;
                    result = cloudDB.insertPokeStop((string)dr[1], (string)dr[2], (Boolean)dr[3], (Boolean)dr[4]);
                    if (result == 0)
                        insertCount++;
                    while (result == 2)
                    {
                        Console.WriteLine("Retry inserting CloudDB");
                        Thread.Sleep(2000);
                        result = cloudDB.insertPokeStop((string)dr[1], (string)dr[2], (Boolean)dr[3], (Boolean)dr[4]);
                        if (result == 0)
                            insertCount++;
                    }
                    Console.WriteLine("[{0}] -> {1},{2},{3},{4},{5}", insertCount, dr[0], dr[1], dr[2], dr[3], dr[4]);
                }

                db.closeDB();
                Console.WriteLine("Completed. Total Read: " + totalRead + ", Total inserted: ");
                Console.ReadKey();
            } catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.ToString());
                Console.WriteLine("Exit...");
                Console.ReadKey();
            }
        }
    }
}

using System;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;
using System.Text.RegularExpressions;

namespace BulkCopy
{
    public class BulkCopy
    {
        private readonly SqlConnection _connection;
        public BulkCopy(SqlConnection connection)
        {
            _connection = connection;
        }
        public bool Start(string targetTableName, DataTable data)
        {
            SqlBulkCopy bulkCopy = null;
            try
            {
                bulkCopy = new SqlBulkCopy(_connection) { DestinationTableName = targetTableName };
                bulkCopy.WriteToServer(data);
                bulkCopy.Close();
                return true;
            }
            catch (SqlException ex)
            {
                if (ex.Message.Contains("Received an invalid column length from the bcp client for colid"))
                {
                    const string pattern = @"\d+";
                    var match = Regex.Match(ex.Message, pattern);
                    var index = Convert.ToInt32(match.Value) - 1;
                    var fi = typeof(SqlBulkCopy).GetField("_sortedColumnMappings",
                        BindingFlags.NonPublic | BindingFlags.Instance);
                    var sortedColumns = fi?.GetValue(bulkCopy);
                    var items = (object[])sortedColumns?.GetType()
                        .GetField("_items", BindingFlags.NonPublic | BindingFlags.Instance)?.GetValue(sortedColumns);
                    if (items == null)
                        return false;
                    var itemdata = items[index].GetType()
                        .GetField("_metadata", BindingFlags.NonPublic | BindingFlags.Instance);
                    if (itemdata == null)
                        return false;
                    var metadata = itemdata.GetValue(items[index]);
                    var column = metadata?.GetType()
                        .GetField("column", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                        ?.GetValue(metadata);
                    var length = metadata?.GetType()
                        .GetField("length", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                        ?.GetValue(metadata);
                    Console.WriteLine($"Column: {column??""} contains data with a length greater than: {length??""} AS Target");
                }
                Console.WriteLine($"{ex} AS Target");
                return false;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ex}");
                return false;
            }
        }
    }
}

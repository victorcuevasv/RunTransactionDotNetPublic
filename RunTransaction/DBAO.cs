using System;
using System.Configuration;
using System.Data.SqlClient;
using Npgsql;

namespace RunTransaction
{
	public class DBAO
	{

		private string sqlDriver;
		private string sqlUrl;

		NpgsqlConnection conn;

		public DBAO ()
		{
			sqlDriver = ConfigurationManager.AppSettings ["flightservice.driver"];
			sqlUrl = ConfigurationManager.AppSettings ["flightservice.url"];
		}

		public void OpenConnection()
		{
			try {
				conn = new NpgsqlConnection(sqlUrl);
				conn.Open();
			}
			catch(Exception e) {
				Console.WriteLine (e.Message);
			}
		}

		public void CloseConnection()
		{
			conn.Close();
		}

		public void ExecuteSelectStatement (string query)
		{
			try {

				NpgsqlCommand stmt = new NpgsqlCommand (query, conn);
				NpgsqlDataReader dr = stmt.ExecuteReader ();

				Console.WriteLine ("");
				for (int i = 0; i < dr.FieldCount; i++) {
					if (i > 0)
						Console.Write (",  ");
					String columnName = dr.GetName (i);
					Console.Write (columnName);
				}
				Console.WriteLine ("");

				while (dr.Read ()) {
					for (int i = 0; i < dr.FieldCount; i++) {
						if (i > 0)
							Console.Write (",  ");
						String columnValue = dr.GetString (i);
						Console.Write (columnValue);
					}
					Console.WriteLine ("");
				}
				dr.Close();
			} catch (Exception e) {
				Console.WriteLine (e.Message);
			}
		}

		public void ExecuteUpdateStatement(String sqlStmtStr) {
			try {
				NpgsqlCommand stmt = new NpgsqlCommand(sqlStmtStr, conn);
				Int32 modified = stmt.ExecuteNonQuery();
				Console.WriteLine(modified + " rows modified");
			}
			catch(Exception e) {
				Console.WriteLine(e.Message);
			}
		}

		public void BeginTransaction(string isolationLevel) {
			try {
				NpgsqlCommand stmt = new NpgsqlCommand("BEGIN ISOLATION LEVEL " + isolationLevel + ";", conn);
				stmt.ExecuteNonQuery();
				Console.WriteLine("begin successful");
			}
			catch(Exception e) {
				Console.WriteLine (e.Message);
			}
		}


		public void CommitTransaction() {
			try {
				NpgsqlCommand stmt = new NpgsqlCommand("COMMIT;", conn);
				stmt.ExecuteNonQuery();
				Console.WriteLine("commit successful");
			}
			catch(Exception e) {
				Console.WriteLine (e.Message);
			}
		}


		public void RollbackTransaction() {
			try {
				NpgsqlCommand stmt = new NpgsqlCommand("ROLLBACK;", conn);
				stmt.ExecuteNonQuery();
				Console.WriteLine("rollback successful");
			}
			catch(Exception e) {
				Console.WriteLine (e.Message);
			}
		}


	}
}


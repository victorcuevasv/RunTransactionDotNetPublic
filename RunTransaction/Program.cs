using System;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Configuration;

namespace RunTransaction
{
	class MainClass
	{

		private const string DBCONFIG_FILENAME = "dbconn.properties";

		private string[] isolationLevels= {"READ UNCOMMITTED", "READ COMMITTED", 
			"REPEATABLE READ", "SERIALIZABLE"};

		private DBAO dbao;

		public MainClass()
		{
			/* prepare the database connection stuff */
			dbao = new DBAO();
			dbao.OpenConnection();
		}

		public void Usage()
		{
			/* prints the choices for commands and parameters */
			Console.WriteLine();
			Console.WriteLine(" *** Please enter one of the following commands *** ");
			Console.WriteLine("> begin transaction");
			Console.WriteLine("> execute sql statement");
			Console.WriteLine("> commit transaction");
			Console.WriteLine("> rollback transaction");
			Console.WriteLine("> quit");
		}

		public string[] Tokenize(string command)
		{
			string pat = "\"([^\"]*)\"|(\\S+)";
			Regex r = new Regex(pat);
			// Match the regular expression pattern against a text string.
			Match m = r.Match(command);
			List<string> tokens = new List<string> ();
			while (m.Success) 
			{
				for (int i = 1; i <= 2; i++) 
				{
					Group g = m.Groups[i];
					if (g.Length > 0)
					{
						tokens.Add (g.Value);
					}
				}
				m = m.NextMatch();
			}
			return tokens.ToArray();
		}

		public void Menu ()
		{
			string command = null;
			while (true) {
				Usage ();
				Console.Write ("> ");
				command = Console.ReadLine ();
				string[] tokens = Tokenize (command.Trim ());
				if (tokens.Length == 0) {
					Console.WriteLine ("Please enter a command");
					continue; // back to top of loop
				}
				if (tokens [0].Equals ("begin")) {
					int retVal = BeginTransactionOption();
					if (retVal == -1)
						continue;
				} else if (tokens [0].Equals ("execute")) {
					Console.WriteLine ();
					Console.WriteLine (" *** Please enter the SQL statement *** ");
					string sqlStmtStr = Console.ReadLine ();
					if (sqlStmtStr.Trim ().ToUpper ().StartsWith ("SELECT")) {
						dbao.ExecuteSelectStatement(sqlStmtStr);
					} else {
						dbao.ExecuteUpdateStatement(sqlStmtStr);
					}
				} else if (tokens [0].Equals ("commit")) {
					dbao.CommitTransaction();
				} else if (tokens [0].Equals ("rollback")) {
					dbao.RollbackTransaction();
				} else if (tokens [0].Equals ("quit")) {
					Environment.Exit (0);
				} else {
					Console.WriteLine ("Error: unrecognized command '" + tokens [0] + "'");
				}
			} //end while
		}

		private int BeginTransactionOption() {
			int retVal = -1;
			Console.WriteLine();
			Console.WriteLine(" *** Please select a transaction isolation level *** ");
			Console.WriteLine("> [1] READ UNCOMMITTED");
			Console.WriteLine("> [2] READ COMMITTED");
			Console.WriteLine("> [3] REPEATABLE READ");
			Console.WriteLine("> [4] SERIALIZABLE");
			try {
				String optionStr = Console.ReadLine();
				int option = Int32.Parse(optionStr);
				if (option >= 1 && option <= 4) {
					dbao.BeginTransaction(isolationLevels[option - 1]);
					retVal = 1;
				}
				else {
					Console.WriteLine("Error: invalid option");
					retVal = -1;
				}
			}
			catch (FormatException e) {
				Console.WriteLine("Error: invalid option");
				retVal = -1;
			}
			return retVal;
		}

		public void closeConnection()
		{
			dbao.CloseConnection();
		}

		public static void Main (string[] args)
		{
			MainClass app = new MainClass ();
			app.Menu ();
			Console.ReadLine ();
		}

	}
}

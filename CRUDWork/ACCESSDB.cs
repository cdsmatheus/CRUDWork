using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CRUDWork
{
    public class ACCESSDB
    {
        public ACCESSDB() { }

        public OleDbConnection CreateDatabaseConnection(string FileLocation, string DatabaseName)
        {
            //Here we create a Standard StringConnection
            string stringConnection = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source="+FileLocation+DatabaseName+";Persist Security Info=False;";
            //Here We connect with the database
            OleDbConnection sqlConnection = new OleDbConnection(stringConnection);
            return sqlConnection;
        }

        public OleDbConnection CreateDatabaseConnection(string FileLocation, string DatabaseName, string DatabasePassword)
        {
            //Here we create a Standard StringConnection
            string stringConnection = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + FileLocation + DatabaseName + ";Persist Security Info=True;Jet OLEDB:Database Password="+DatabasePassword+";";
            //Here We connect with the database
            OleDbConnection sqlConnection = new OleDbConnection(stringConnection);
            return sqlConnection;
        }

        public DataTable DataTableSelectCommand(string[] ColumnNames, string TableName, OleDbConnection _OleDbConnection)
        {
            //Here, the list of fields passed by the user, will be transfered from an Array to a single string, like a Line
            //First, We count the number of columns passed
            int counter = ColumnNames.Length;
            //Create a String to Store the COlumns Line
            string Columns = string.Empty;
            //Start the counter having as a Limit, the number of columns passed by the user
            for (int i = 0; i <= counter - 1; i++)
            {
                //For the First Column, this If command skips the adding of a comma
                if (Columns == String.Empty)
                {
                    Columns = ColumnNames[i];
                }
                //Here, new commas will by added when needed
                else
                {

                    Columns = Columns + ", " + ColumnNames[i];
                }
            }
            //Here, the select command is builded
            string Command = "SELECT " + Columns + " FROM " + TableName;
            //Build a New Connection
            OleDbCommand cmd = new OleDbCommand(Command, _OleDbConnection);
            //Open Connection
            _OleDbConnection.Open();
            //Creates a new DataTable
            DataTable dt = new DataTable();
            //Fill the DataTable
            dt.Load(cmd.ExecuteReader());
            //Close Connection
            _OleDbConnection.Close();
            //Returns the Datatable
            return dt;
        }

        public DataTable DataTableSelectCommandWithParameter(string[] ColumnNames, string TableName, OleDbConnection _OleDbConnection, string stringSqlParameter)
        {
            //Here, the list of fields passed by the user, will be transfered from an Array to a single string, like a Line
            //First, We count the number of columns passed
            int counter = ColumnNames.Length;
            //Create a String to Store the COlumns Line
            string Columns = string.Empty;
            //Start the counter having as a Limit, the number of columns passed by the user
            for (int i = 0; i <= counter - 1; i++)
            {
                //For the First Column, this If command skips the adding of a comma
                if (Columns == String.Empty)
                {
                    Columns = ColumnNames[i];
                }
                //Here, new commas will by added when needed
                else
                {

                    Columns = Columns + ", " + ColumnNames[i];
                }
            }
            //Here, the select command is builded
            string Command = "SELECT " + Columns + " FROM " + TableName + " " + stringSqlParameter;
            //Build a New Connection
            OleDbCommand cmd = new OleDbCommand(Command, _OleDbConnection);
            //Open Connection
            _OleDbConnection.Open();
            //Creates a new DataTable
            DataTable dt = new DataTable();
            //Fill the DataTable
            dt.Load(cmd.ExecuteReader());
            //Close Connection
            _OleDbConnection.Close();
            //Returns the Datatable
            return dt;
        }

        public DataSet DataSetSelectCommand(string[] ColumnNames, string TableName, OleDbConnection _OleDbConnection)
        {
            //Here, the list of fields passed by the user, will be transfered from an Array to a single string, like a Line
            //First, We count the number of columns passed
            int counter = ColumnNames.Length;
            //Create a String to Store the COlumns Line
            string Columns = string.Empty;
            //Start the counter having as a Limit, the number of columns passed by the user
            for (int i = 0; i <= counter - 1; i++)
            {
                //For the First Column, this If command skips the adding of a comma
                if (Columns == String.Empty)
                {
                    Columns = ColumnNames[i];
                }
                //Here, new commas will by added when needed
                else
                {

                    Columns = Columns + ", " + ColumnNames[i];
                }
            }
            //Here, the select command is builded
            string Command = "SELECT " + Columns + " FROM " + TableName;
            //Build a New Connection
            OleDbCommand cmd = new OleDbCommand(Command, _OleDbConnection);
            //Open Connection
            _OleDbConnection.Open();
            //Creates a OleDbDataAdapter
            OleDbDataAdapter da = new OleDbDataAdapter(Command, _OleDbConnection);
            //Creates the dataSet
            DataSet ds = new DataSet(Command);
            da.Fill(ds, TableName);
            return ds;
        }

        public DataSet DataSetSelectCommandWithParameter(string[] ColumnNames, string TableName, OleDbConnection _OleDbConnection, string _StringSqlParameter)
        {
            //Here, the list of fields passed by the user, will be transfered from an Array to a single string, like a Line
            //First, We count the number of columns passed
            int counter = ColumnNames.Length;
            //Create a String to Store the COlumns Line
            string Columns = string.Empty;
            //Start the counter having as a Limit, the number of columns passed by the user
            for (int i = 0; i <= counter - 1; i++)
            {
                //For the First Column, this If command skips the adding of a comma
                if (Columns == String.Empty)
                {
                    Columns = ColumnNames[i];
                }
                //Here, new commas will by added when needed
                else
                {

                    Columns = Columns + ", " + ColumnNames[i];
                }
            }
            //Here, the select command is builded
            string Command = "SELECT " + Columns + " FROM " + TableName + " " + _StringSqlParameter;
            //Build a New Connection
            OleDbCommand cmd = new OleDbCommand(Command, _OleDbConnection);
            //Open Connection
            _OleDbConnection.Open();
            //Creates a OleDbDataAdapter
            OleDbDataAdapter da = new OleDbDataAdapter(Command, _OleDbConnection);
            //Creates the dataSet
            DataSet ds = new DataSet(Command);
            da.Fill(ds, TableName);
            return ds;
        }

        public OleDbDataReader DataReaderSelectCommand(string[] ColumnNames, string TableName, OleDbConnection _OleDbConnection)
        {
            //Here, the list of fields passed by the user, will be transfered from an Array to a single string, like a Line
            //First, We count the number of columns passed
            int counter = ColumnNames.Length;
            //Create a String to Store the COlumns Line
            string Columns = string.Empty;
            //Start the counter having as a Limit, the number of columns passed by the user
            for (int i = 0; i <= counter - 1; i++)
            {
                //For the First Column, this If command skips the adding of a comma
                if (Columns == String.Empty)
                {
                    Columns = ColumnNames[i];
                }
                //Here, new commas will by added when needed
                else
                {

                    Columns = Columns + ", " + ColumnNames[i];
                }
            }
            //Here, the select command is builded
            string Command = "SELECT " + Columns + " FROM " + TableName;
            //Build a New Connection
            OleDbCommand cmd = new OleDbCommand(Command, _OleDbConnection);
            //Open Connection
            _OleDbConnection.Open();
            //Creates a OleDbDataAdapter
            OleDbDataReader reader = cmd.ExecuteReader();
            return reader;
        }

        public OleDbDataReader DataReaderSelectCommandWithParameter(string[] ColumnNames, string TableName, OleDbConnection _OleDbConnection, string _StringSqlParameter)
        {
            //Here, the list of fields passed by the user, will be transfered from an Array to a single string, like a Line
            //First, We count the number of columns passed
            int counter = ColumnNames.Length;
            //Create a String to Store the COlumns Line
            string Columns = string.Empty;
            //Start the counter having as a Limit, the number of columns passed by the user
            for (int i = 0; i <= counter - 1; i++)
            {
                //For the First Column, this If command skips the adding of a comma
                if (Columns == String.Empty)
                {
                    Columns = ColumnNames[i];
                }
                //Here, new commas will by added when needed
                else
                {

                    Columns = Columns + ", " + ColumnNames[i];
                }
            }
            //Here, the select command is builded
            string Command = "SELECT " + Columns + " FROM " + TableName + " " + _StringSqlParameter;
            //Build a New Connection
            OleDbCommand cmd = new OleDbCommand(Command, _OleDbConnection);
            //Open Connection
            _OleDbConnection.Open();
            //Creates a OleDbDataAdapter
            OleDbDataReader reader = cmd.ExecuteReader();
            return reader;
        }

        public Boolean InsertData(string TableName, string[] TableColumns, string[] ValuesToInsert, OleDbConnection sqlConnection)
        {

            int ColumnsCounter = TableColumns.Length;
            int ValuesCounter = ValuesToInsert.Length;
            string columns = string.Empty;
            string values = string.Empty;
            //This portion of the code, set the list of columns names into a line
            for (int i = 0; i <= ColumnsCounter - 1; i++)
            {
                if (columns == String.Empty)
                {
                    columns = TableColumns[i].ToString();
                }
                else
                {
                    columns = columns + ", " + TableColumns[i].ToString();
                }
            }
            //This portion of the code, set the list of Parameters for the Insert Command
            for (int j = 0; j <= ValuesCounter - 1; j++)
            {
                if (values == String.Empty)
                {
                    values = "@" + TableColumns[j].ToString();
                }
                else
                {
                    values = values + ", " + "@" + TableColumns[j].ToString();
                }
            }
            //Insert Command is ready
            string sqlCmd = "INSERT INTO " + TableName + "(" + columns + ")" + " VALUES " + "(" + values + ")";
            //Opens the connection
            sqlConnection.Open();
            OleDbCommand cmd = new OleDbCommand(sqlCmd, sqlConnection);
            //Associates every parameter with its value
            for (int j = 0; j <= ValuesCounter - 1; j++)
            {
                cmd.Parameters.AddWithValue("@" + TableColumns[j].ToString(), ValuesToInsert[j].ToString());
            }
            cmd.ExecuteNonQuery();
            sqlConnection.Close();
            return true;

        }

        public Boolean UpdateData(string TableName, string[] TableColumns, string[] ValuesToUpdate, string WhereParameter, OleDbConnection sqlConnection)
        {
            string ValuesWithParameters = String.Empty;
            int count = TableColumns.Length;
            for (int i = 0; i <= count - 1; i++)
            {
                if (ValuesWithParameters == String.Empty)
                {
                    ValuesWithParameters = TableColumns[i].ToString() + "=@" + TableColumns[i].ToString() + ",";
                }
                else
                {
                    ValuesWithParameters = ValuesWithParameters + ", " + TableColumns[i].ToString() + "=@" + TableColumns[i].ToString() + ",";
                }

            }
            string sqlCommand = "UPDATE " + TableName + " SET " + ValuesWithParameters + " WHERE " + WhereParameter;
            OleDbCommand cmd = new OleDbCommand(sqlCommand, sqlConnection);
            for (int j = 0; j <= count - 1; j++)
            {
                cmd.Parameters.AddWithValue("@" + TableColumns[j].ToString(), ValuesToUpdate[j].ToString());
            }
            cmd.ExecuteNonQuery();
            return true;
        }

        public Boolean DeleteData(string TableName, string WhereParameter, OleDbConnection sqlConnection)
        {
            string sqlCommand = "DELETE From " + TableName + "WHERE" + WhereParameter;
            OleDbCommand cmd = new OleDbCommand(sqlCommand, sqlConnection);
            sqlConnection.Open();
            cmd.ExecuteNonQuery();
            sqlConnection.Close();
            return true;
        }

        public Boolean RunCommand(string OleDbCommand, OleDbConnection connection)
        {
            OleDbCommand cmd = new OleDbCommand(OleDbCommand, connection);
            connection.Open();
            cmd.ExecuteNonQuery();
            connection.Close();
            return true;
        }
    }
}


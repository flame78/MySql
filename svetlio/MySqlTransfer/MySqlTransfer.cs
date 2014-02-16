using MySql.Data.MySqlClient;
using System.IO;
using System.Collections.Generic;
using System;

static public class MySqlTransfer
{

    static public void DatFile(string server, string dBName, string table, string user, string pass, string path)
    {
        MySqlConnection conn;
        StreamReader reader;
        Queue<string> lines = new Queue<string>();
        MySqlTransaction sqlTran = null;
        MySqlCommand cmd;

        try
        {
            DirectoryInfo di = new DirectoryInfo(path);
            var directories = di.GetFiles("*.dat", SearchOption.TopDirectoryOnly);


            conn = OpenConnection(server, dBName, user, pass);
            cmd = new MySqlCommand("SET autocommit = 0", conn);
            sqlTran = conn.BeginTransaction();

            foreach (var file in directories)
            {
                reader = new StreamReader(file.FullName);

                using (reader)
                {
                    while (reader.EndOfStream != true)
                    {
                        lines.Enqueue(reader.ReadLine());
                    }
                }


                while (lines.Count > 0)
                {
                    string[] tokens = new string[8];
                    tokens = lines.Dequeue().Split('|');

                    for (int i = 0; i < 8; i++)
                    {
                        tokens[i] = tokens[i].Trim();
                    }

                    cmd = new MySqlCommand(string.Format("insert into {0} (GameEnd,GameType,Price,Discount,GameTime,Operator,GameTable,Comments) values(@GameEnd,@GameType,@Price,@Discount,@GameTime,@Operator,@GameTable,@Comments)", table), conn, sqlTran);


                    cmd.Parameters.Add("@GameEnd", MySqlDbType.DateTime).Value = tokens[0];
                    cmd.Parameters.Add("@GameType", MySqlDbType.Text).Value = tokens[1];
                    if (tokens[2] == "") cmd.Parameters.Add("@Price", MySqlDbType.Decimal).Value = DBNull.Value;
                    else cmd.Parameters.Add("@Price", MySqlDbType.Decimal).Value = tokens[2];
                    cmd.Parameters.Add("@Discount", MySqlDbType.Byte).Value = tokens[3];
                    cmd.Parameters.Add("@GameTime", MySqlDbType.Int16).Value = tokens[4];
                    cmd.Parameters.Add("@Operator", MySqlDbType.Text).Value = tokens[5];
                    cmd.Parameters.Add("@GameTable", MySqlDbType.Text).Value = tokens[6];
                    cmd.Parameters.Add("@Comments", MySqlDbType.Text).Value = tokens[7];

                    cmd.ExecuteNonQuery();
                }
                sqlTran.Commit();
                //File.Move(file.FullName, file.FullName + ".proc");
            }
            conn.Close();
        }
        catch (MySqlException ex)
        {
            WriteError(ex);
            try { sqlTran.Rollback(); conn.Close(); }
            catch { }
        }
        catch (Exception ex)
        {
            WriteError(ex);
        }
    }

    private static MySqlConnection OpenConnection(string server, string dBName, string user, string pass)
    {
        MySqlConnection result = new MySqlConnection();

        string connStr = String.Format("server={0}; database={1}; user id={2}; password={3}; pooling=false",
             server, dBName, user, pass);

        try
        {
            result = new MySqlConnection(connStr);
            result.Open();
        }
        catch (Exception ex)
        {
            WriteError(ex);
        }

        return result;
    }

    private static void WriteError(Exception ex)
    {
        StreamWriter logFile = new StreamWriter("log.txt", true);
        using (logFile)
        {
            logFile.WriteLine(DateTime.Now.ToString());
            logFile.WriteLine(ex.ToString());
        }
    }

    static public void CreateMyDbTbl(string server, string user, string pass)
    {
        MySqlConnection conn;


        conn = OpenConnection(server, "", user, pass);

        try
        {
            MySqlCommand cmd = new MySqlCommand(@"CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET latin1 */;
            CREATE TABLE  `test`.`tbl` (
             `ID` int(11) NOT NULL AUTO_INCREMENT,
             `GameEnd` datetime NOT NULL ,
             `GameType` text,
             `Price` decimal(3,2) ,
             `Discount` tinyint(4) ,
             `GameTime` smallint(6) ,
             `Operator` text,
             `GameTable` text,
             `Comments` text,
             PRIMARY KEY (`Id`)
           ) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='Events';", conn);

            cmd.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            WriteError(ex);
        }

        conn.Close();
    }

}

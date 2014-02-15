
using MySql.Data.MySqlClient;
using System.Data;
using System.IO;
using System.Collections.Generic;
using System;

static public class MySqlTransfer
{
    static public void DatFile(string server, string dBName, string table, string user, string pass)
    {
        MySqlConnection conn;
        StreamReader reader;
        Queue<string> lines = new Queue<string>();

        try
        {
            DirectoryInfo di = new DirectoryInfo(@"..\..\");
            var directories = di.GetFiles("*.dat", SearchOption.TopDirectoryOnly);

            conn = OpenConnection(server, dBName, user, pass);

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

                    MySqlCommand cmd = new MySqlCommand(string.Format("insert into {0} (GameEnd,GameType,Price,Discount,GameTime,Operator,GameTable,Comments) values(@GameEnd,@GameType,@Price,@Discount,@GameTime,@Operator,@GameTable,@Comments)", table), conn);

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
                File.Move(file.FullName, file.FullName + ".proc");
            }
            conn.Close();
        }
        catch (Exception)
        {
            Console.WriteLine("Bum");
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
        catch (MySqlException ex)
        {
            Console.WriteLine("Error connecting to the server: " + ex.Message);
        }

        return result;
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
            catch (Exception)
            {
                Console.WriteLine("Ne stava :) Ve4e ima takava baza");
            }

            conn.Close();
        

        

        
    }
}

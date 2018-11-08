using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace UpdateTallyServer
{
    public partial class Form1 : Form
    {
        OdbcConnection DbConnection;
        OdbcCommand DbCommand,OpenCompanycmd;
        OdbcDataReader DbReader,OpenCompanyReader;
        String OpenCompanyName;
        Boolean OdbcConOpen = false;
        int city_id;
        int NumberOfCompaniesOpen=0;
        SqlConnection sqlLocal;
        SqlCommand cmdRead, cmdupdate, cmdinsert,cmdReadConn;
        SqlDataAdapter adapter;
        SqlDataReader reader,connectionReader;

        public Form1()
        {
            InitializeComponent();
            sqlLocal = new SqlConnection(ConfigurationManager.ConnectionStrings["tallyserver"].ConnectionString);
        }
        private void Form1_Load(object sender, EventArgs e)
        {
            for (int i = 1; i <= 21; i++)
            {
                OdbcConOpen = OpenOdbcConnection(i);
                Retrivedata();
                DbConnection.Close();
            }
            Application.Exit();

        }

        private void Retrivedata()
        {
            try
            {
                double balance;
                String _Parent;
                String _PrimaryGroup;
                String ClosingBalance;
                String OpeningBalance;
                String Name;
                String Alias;

                //check odbc connection
                if (OdbcConOpen)
                {
                    OpenCompanyName=getCompany_name();
                    //if only one comapny is open
                    if (NumberOfCompaniesOpen == 1)
                    {
                        sqlLocal.Open();
                        if ((city_id = getCity_id(OpenCompanyName)) != 0)
                        {
                            //new create command
                            DbCommand = DbConnection.CreateCommand();
                            DbCommand.CommandText = "SELECT $$Alias,`$Name`,`$OpeningBalance`,`$_ClosingBalance`,`$Parent`,$_PrimaryGroup FROM Ledger where $_PrimaryGroup='Sundry Debtors' and $$Alias='OP PT-NOI'";
                            DbReader = DbCommand.ExecuteReader();
                            //once the odbc connection is open open sqlconnection


                            while (DbReader.Read())
                            {
                                Alias = DbReader.GetValue(0).ToString().Replace("\r\n","");
                                Name = DbReader.GetValue(1).ToString().Replace("'", "");
                                OpeningBalance = DbReader.GetValue(2).ToString();
                                ClosingBalance = DbReader.GetValue(3).ToString();
                                _Parent = DbReader.GetValue(4).ToString();
                                _PrimaryGroup = DbReader.GetValue(5).ToString();
                                if (OpenCompanyName == getCompany_name())
                                {
                                    //if only one comapny is open
                                    if (NumberOfCompaniesOpen == 1)
                                    {
                                        city_id = getCity_id(getCompany_name());
                                        if (ClosingBalance != "")
                                        {
                                            if (Convert.ToDouble(DbReader.GetValue(3).ToString()) < 0)
                                            {
                                                balance = Math.Abs(Convert.ToDouble(DbReader.GetValue(3).ToString()));
                                            }
                                            else
                                            {
                                                balance = Convert.ToDouble("-" + DbReader.GetValue(3).ToString());
                                            }
                                        }
                                        else
                                        {
                                            balance = 0;
                                        }
                                        cmdRead = new SqlCommand("select * from TallyPartyBalance where City_ID ='" + city_id + "' and TallyPartyName = '" + Name + "'", sqlLocal);
                                        reader = cmdRead.ExecuteReader();
                                        if (reader.Read())
                                        {
                                            //if (reader[4].ToString() != balance.ToString())
                                            //{
                                                reader.Close();
                                                cmdupdate = new SqlCommand("update TallyPartyBalance set balamt='" + balance + "',updatedate='" + DateTime.Now.ToString("yyyyMMdd") + "',updatetime='" + DateTime.Now.ToString("yyyyMMdd") + "',party_code='" + Alias + "' where tallypartyname='" + Name + "' and city_id=" + city_id + "", sqlLocal);
                                                cmdupdate.ExecuteNonQuery();

                                            //}

                                        }
                                        else
                                        {
                                            reader.Close();
                                            cmdinsert = new SqlCommand("insert into TallyPartyBalance (party_code,tallypartyname,balamt,updatedate,updatetime,city_id,parent,_primaryGroup) values('"+Alias+"','" + Name + "','" + balance + "','" + DateTime.Now.ToString("yyyyMMdd") + "','" + DateTime.Now.ToString("yyyyMMdd") + "','" + city_id + "','" + _Parent + "','" + _PrimaryGroup + "')", sqlLocal);
                                            cmdinsert.ExecuteNonQuery();

                                        }
                                        reader.Close();
                                    }
                                }


                            }
                            DbReader.Close();
                            DbCommand.Dispose();
                            sqlLocal.Close();
                        }
                    }//only one company open End

                }//odbcconopen condition End
            }
            catch (Exception ea)
            {
               
            }

        }
        private String getCompany_name()
        {
            string Name = "";
            NumberOfCompaniesOpen=0;
            OpenCompanycmd = DbConnection.CreateCommand();
            OpenCompanycmd.CommandText = "SELECT `$Name` FROM  Company";
            OpenCompanyReader = OpenCompanycmd.ExecuteReader();

            while(OpenCompanyReader.Read())
            {
                
                Name = OpenCompanyReader.GetValue(0).ToString();
                NumberOfCompaniesOpen++;
            }
            OpenCompanyReader.Close();
            OpenCompanycmd.Dispose();

            return Name;
        }

        private int getCity_id(String companyName)
        {
            int id = 0;
            //sqlLocal.Close();
            //sqlLocal.Open();
            cmdReadConn = new SqlCommand("select city_id from ConfigureMyTallyBuddy where CompanyName='" +companyName+ "'", sqlLocal);
            connectionReader = cmdReadConn.ExecuteReader();
            while(connectionReader.Read())
            {
               
                id = Convert.ToInt32(connectionReader[0].ToString());
            }
            connectionReader.Close();
            //sqlLocal.Close();
            return id;
        }
        
        //open odbc connection
        private Boolean OpenOdbcConnection(int caseSwitch)
        {
            Boolean Result = false;
            try
            {
                switch (caseSwitch)
                {
                    case 1:
                        DbConnection = new OdbcConnection("DSN=TallyODBC64_18001");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 2:
                        DbConnection = new OdbcConnection("DSN=TallyODBC64_18002");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 3:
                        DbConnection = new OdbcConnection("DSN=TallyODBC64_18003");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 4:
                        DbConnection = new OdbcConnection("DSN=TallyODBC64_18004");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 5:
                        DbConnection = new OdbcConnection("DSN=TallyODBC64_18005");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 6:
                        DbConnection = new OdbcConnection("DSN=TallyODBC64_18006");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 7:
                        DbConnection = new OdbcConnection("DSN=TallyODBC64_18007");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 8:
                        DbConnection = new OdbcConnection("DSN=TallyODBC64_18008");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 9:
                        DbConnection = new OdbcConnection("DSN=TallyODBC64_18009");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 10:
                        DbConnection = new OdbcConnection("DSN=TallyODBC64_18010");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 11:
                        DbConnection = new OdbcConnection("DSN=TallyODBC64_18011");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 12:
                        DbConnection = new OdbcConnection("DSN=TallyODBC64_18012");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 13:
                        DbConnection = new OdbcConnection("DSN=TallyODBC64_18013");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 14:
                        DbConnection = new OdbcConnection("DSN=TallyODBC64_18014");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 15:
                        DbConnection = new OdbcConnection("DSN=TallyODBC64_18015");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 16:
                        DbConnection = new OdbcConnection("DSN=TallyODBC64_18016");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 17:
                        DbConnection = new OdbcConnection("DSN=TallyODBC64_18017");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 18:
                        DbConnection = new OdbcConnection("DSN=TallyODBC64_18018");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 19:
                        DbConnection = new OdbcConnection("DSN=TallyODBC_9001");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 20:
                        DbConnection = new OdbcConnection("DSN=TallyODBC_9002");
                        DbConnection.Open();
                        Result = true;
                        break;
                    case 21:
                        DbConnection = new OdbcConnection("DSN=TallyODBC64_18000");
                        DbConnection.Open();
                        Result = true;
                        break;
                    default:
                        break;
                }
            }
            catch (Exception ea)
            {

            }
            return Result;
        }

    }
}

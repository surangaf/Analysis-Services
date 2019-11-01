using System;
using Microsoft.AnalysisServices.Tabular;
using Microsoft.IdentityModel.Clients.ActiveDirectory;

namespace AAS_Refresh
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                //Set up all the variables needed
                //In this example we are using the same clientId and secret for both access to the Analysis Services model AND to the data source
                string clientId = "abcdefgh-1111-2222-3333-ijklmnopqrst";
                string aadTenantId = "abcdefgh-1111-2222-3333-ijklmnopqrst";
                string clientSecretKey = "abcde12345fghij67890klmno12345pq";
                string AadInstance = "https://login.windows.net/{0}";
                string ResourceId = "https://database.windows.net/";

                /**************************************
                 * 
                 * Obtain a fresh Access Token
                 * 
                 **************************************/
                AuthenticationContext authenticationContext = new AuthenticationContext(string.Format(AadInstance, aadTenantId));
                ClientCredential clientCredential = new ClientCredential(clientId, clientSecretKey);

                DateTime startTime = DateTime.Now;
                Console.WriteLine("Getting new OAuth AccessToken. Time: " + String.Format("{0:mm:ss.fff}", startTime));

                AuthenticationResult authenticationResult = authenticationContext.AcquireTokenAsync(ResourceId, clientCredential).Result;

                DateTime endTime = DateTime.Now;
                Console.WriteLine("Got token at " + String.Format("{0:mm:ss.fff}", endTime));

                Console.WriteLine("Total time to get token in milliseconds " + (endTime - startTime).TotalMilliseconds);


                /**************************************
                 * 
                 * Update the Datasource credentials and process a table
                 * 
                 **************************************/

                //Set up the variables needed for model processing
                string aasDatabaseName = "myAASDatabase";
                string aasServer = "asazure://<region>.asazure.windows.net/<aasservername>";          
                //Example of connection string used to connect to Azure Analysis Services using a service principal
                string aasConnectionString = string.Format("Provider=MSOLAP;User ID={0};Password={1};Initial Catalog={2};Data Source={3}", "app:" + clientId, clientSecretKey, aasDatabaseName, aasServer);
                //The name of the Azure Analysis Services model datasource
                string aasDataSourceName = "<Data Source Name>";
                string tableToProcess = "TableToProcess";

                Server s = new Server();
                s.Connect(aasConnectionString);

                Database db = s.Databases[aasDatabaseName];
                Model m = db.Model;
                StructuredDataSource ds = (StructuredDataSource) m.DataSources.Find(aasDataSourceName);

                //Replace the datasource credential with a new instance containing the fresh access token
                ds.Credential = new Credential{
                    AuthenticationKind = AuthenticationKind.OAuth2,
                    [CredentialProperty.AccessToken] = authenticationResult.AccessToken,
                    [CredentialProperty.Expires] = authenticationResult.ExpiresOn.ToLocalTime().ToString(),
                    [CredentialProperty.EncryptConnection] = true
                };

                //process a table
                m.Tables[tableToProcess].RequestRefresh(Microsoft.AnalysisServices.Tabular.RefreshType.Full);

                Console.WriteLine("Saving changes to model....");
                startTime = DateTime.Now;
                m.SaveChanges();
                endTime = DateTime.Now;
                Console.WriteLine("Process is successful. Time in milliseconds: " + (endTime - startTime).TotalMilliseconds);

                s.Disconnect();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            Console.WriteLine("Complete");
            Console.ReadLine();

        }

    }
}





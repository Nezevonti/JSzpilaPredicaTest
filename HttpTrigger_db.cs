using System;
using System.IO;
using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System.Text.Json;


using System.Data.SqlClient;
using System.Threading.Tasks;

using System.Net.Http;
using Microsoft.AspNetCore.Http;

namespace Company.Function
{
    public static class HttpTrigger_db
    {
        public class Root{
            public char table {get; set;}
            public string no {get; set;}
            public string effectiveDate { get; set; } 
            public List<Rate> rates {get; set;}
        };

        public class Rate{
            public string currency {get; set;}
            public string code {get; set;}
            public double mid {get; set;}
        };

        private static async Task<string> GetRates(){
            string requestURL = "http://api.nbp.pl/api/exchangerates/tables/A/?format=json";

            //create new client and call the API
            HttpClient client = new HttpClient();
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, requestURL);
            HttpResponseMessage response = await client.SendAsync(request);
            
            //read the response, save as string
            string responsestring = await response.Content.ReadAsStringAsync();

            return responsestring;
        }

        private static async Task<IActionResult> SaveToDb(Root xChangeTables, ILogger log){

            int totalRowsChanged = 0;
            //get the sql connection string
            var connection_string = Environment.GetEnvironmentVariable("sqldb_connection");

            //connect to the DB
            using (SqlConnection conn = new SqlConnection(connection_string))
            {
                conn.Open();
                log.LogInformation("Connection opened");

                //for each of the currencies write into the DB, adding new row
                foreach(Rate r in xChangeTables.rates){
                    var text = GetSQLCommText(r.code,xChangeTables.effectiveDate,r.mid);
                    log.LogInformation(r.code);

                    using (SqlCommand cmd = new SqlCommand(text, conn))
                    {
                        // Execute the command and log the # rows affected.
                        var rows = await cmd.ExecuteNonQueryAsync();
                        log.LogInformation($"{rows} rows were updated");
                        totalRowsChanged+=rows;
                    }
                }
                

                
            }

            return new OkObjectResult(totalRowsChanged + " rows added");

        }

        private static string GetSQLCommText(string code,string date, double mid){
            //change mid to string and replace , with . as to not confuse the sqp server
            string midStr = mid.ToString();
            midStr = midStr.Replace(',','.');
            //create the SQL querry
            string commtext = "INSERT INTO dbo.CurrencyExchangerate (FromCurrencyId, ToCurrencyId, RateDate, ExchangeRate) VALUES(1, (Select CurrencyId From [dbo].[Currencies] Where ISO_Code='" + code + "'), convert(date,'" + date + "') , "+ midStr +");";

            return commtext;
        }

        [FunctionName("HttpTrigger_db")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            //---- nbp api ----
            

            //get the JSON with x-change rates 
            string JsonApiResponseString = GetRates().Result;

            //parse the json into object
            Root? currentTables = JsonSerializer.Deserialize<List<Root>>(JsonApiResponseString)[0];
            log.LogInformation("Exchange rate tables obtained and deserialize");

            //save to DB
            IActionResult savingResult = SaveToDb(currentTables,log).Result;

            return savingResult;
        }
    }
}

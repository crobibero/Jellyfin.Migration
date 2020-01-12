using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Serilog;

namespace Migration
{
    internal static class Program
    {

        private const string SettingsKey = "Settings";
        private const string SourceUrlKey = "Source:Url";
        private const string SourceApiKeyKey = "Source:ApiKey";
        private const string DestinationUrlKey = "Destination:Url";
        private const string DestinationApiKeyKey = "Destination:ApiKey";
        private const string DestinationAdminUsernameKey = "Destination:AdminUsername";

        private static string _sourceUrl;
        private static string _sourceApiKey;
        private static string _destinationUrl;
        private static string _destinationApiKey;

        private static Dictionary<string, User> _sourceUsers;
        private static Dictionary<string, User> _destinationUsers;
        
        private static readonly DataTable DestinationMediaTable = new DataTable();

        private static readonly HttpClient Client = new HttpClient();

        private static async Task Main()
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Verbose()
                .WriteTo.File(Path.Join("log", "Migration..log"), rollingInterval: RollingInterval.Day)
                .WriteTo.Console()
                .CreateLogger();

            try
            {
                var config = new ConfigurationBuilder()
                    .AddJsonFile("appsettings.json", false)
                    .Build();

                var settings = config.GetSection(SettingsKey);
                _sourceUrl = settings[SourceUrlKey]?.TrimEnd('/');
                _sourceApiKey = settings[SourceApiKeyKey];
                _destinationUrl = settings[DestinationUrlKey]?.TrimEnd('/');
                _destinationApiKey = settings[DestinationApiKeyKey];
                var destinationAdminUsername = settings[DestinationAdminUsernameKey];

                Log.Verbose(new string('=', 50));
                Log.Verbose("Configuration: Source Url: {0}", _sourceUrl);
                Log.Verbose("Configuration: Source ApiKey: {0}", _sourceApiKey);
                Log.Verbose("Configuration: Destination Url: {0}", _destinationUrl);
                Log.Verbose("Configuration: Destination ApiKey: {0}", _destinationApiKey);
                Log.Verbose(new string('=', 50));
                Thread.Sleep(5 * 1000);

                if (string.IsNullOrEmpty(_sourceUrl))
                    throw new NullReferenceException(SourceUrlKey);
                if (string.IsNullOrEmpty(_sourceApiKey))
                    throw new NullReferenceException(SourceApiKeyKey);
                if (string.IsNullOrEmpty(_destinationUrl))
                    throw new NullReferenceException(DestinationUrlKey);
                if (string.IsNullOrEmpty(_destinationApiKey))
                    throw new NullReferenceException(DestinationApiKeyKey);

                var sourceUsersTask = GetUsersAsync(_sourceUrl, _sourceApiKey);
                var destinationUsersTask = GetUsersAsync(_destinationUrl, _destinationApiKey);

                await Task.WhenAll(sourceUsersTask, destinationUsersTask);
                _sourceUsers = await sourceUsersTask;
                _destinationUsers = await GetUsersAsync(_destinationUrl, _destinationApiKey);
                
                Log.Information("Source User Count: {0}", _sourceUsers.Count);
                Log.Information("Destination User Count: {0}", _destinationUsers.Count);

                Guid destinationAdminId;
                if (_destinationUsers.ContainsKey(destinationAdminUsername))
                {
                    destinationAdminId = _destinationUsers[destinationAdminUsername].Id;
                }
                else
                {
                    Log.Error("Destination Admin {0} not found", destinationAdminUsername);
                    return;
                }
                
                DestinationMediaTable.Columns.Add("imdbId", typeof(string));
                DestinationMediaTable.Columns.Add("tvdbId", typeof(string));
                DestinationMediaTable.Columns.Add("id", typeof(string));
                await PopulateDestinationMediaAsync(destinationAdminId);

                foreach (var (username, user) in _sourceUsers)
                {
                    if (!_destinationUsers.ContainsKey(username))
                    {
                        Log.Warning("User not found in destination: {0}", username);
                        return;
                    }

                    Log.Information("Starting User: {0}", username);
                    var (imdb, tvdb) = await GetWatchedMediaAsync(user.Id);
                    await SetWatchedStatus(_destinationUsers[username].Id, imdb, tvdb);
                }
            }
            catch (Exception e)
            {
                Log.Fatal(e, "Fatal Error");
            }
        }

        /// <summary>
        ///     Gets list of users from server
        /// </summary>
        /// <param name="url">Server Url to get from</param>
        /// <param name="apiKey">ApiKey to use</param>
        /// <returns>List of users</returns>
        private static async Task<Dictionary<string, User>> GetUsersAsync(string url, string apiKey)
        {
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
                RequestUri = new Uri($"{url}/Users?api_key={apiKey}")
            };

            var response = await Client.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var responseStr = await response.Content.ReadAsStringAsync();
            return JsonConvert.DeserializeObject<List<User>>(responseStr)
                .ToDictionary(o => o.Name, o => o);
        }

        private static async Task PopulateDestinationMediaAsync(Guid adminUserId)
        {
            var baseUrl =
                $"{_destinationUrl}/Users/{adminUserId}/Items?Fields=ProviderIds&Recursive=true&ExcludeItemTypes=Studio%2CPerson%2CPlaylistsFolder%2CUserView%2CGenre%2CProgram&api_key={_destinationApiKey}";

            int count,
                offset = 0;
            const int limit = 500;

            do
            {
                var partUrl = baseUrl + $"&StartIndex={offset}&Limit={limit}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                    RequestUri = new Uri(partUrl)
                };

                var response = await Client.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var responseStr = await response.Content.ReadAsStringAsync();
                var items = JsonConvert.DeserializeObject<MediaContainer>(responseStr).Items;
                count = items.Count;

                foreach (var item in items)
                {
                    var imdbId = item.ProviderIds?.imdb;
                    var tvdbId = item.ProviderIds?.tvdb;

                    if (imdbId == null && tvdbId == null)
                        continue;
                    
                    var row = DestinationMediaTable.NewRow();
                    row["imdbId"] = imdbId;
                    row["tvdbId"] = tvdbId;
                    row["id"] = item.Id;

                    DestinationMediaTable.Rows.Add(row);
                }

                Log.Information("[PopulateDestinationMedia]\t Media Count: {itemCount}", count);
                offset += limit;
            } while (count > 0);
        }

        private static async Task<(HashSet<string> imdb, HashSet<string> tvdb)> GetWatchedMediaAsync(Guid userId)
        {
            var baseUrl =
                $"{_sourceUrl}/Users/{userId}/Items?Fields=ProviderIds&Recursive=true&IsPlayed=true&api_key={_sourceApiKey}";

            int count,
                offset = 0;
            const int limit = 500;

            var imdb = new HashSet<string>();
            var tvdb = new HashSet<string>();

            do
            {
                var partUrl = baseUrl + $"&StartIndex={offset}&Limit={limit}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                    RequestUri = new Uri(partUrl)
                };

                var response = await Client.SendAsync(request);
                response.EnsureSuccessStatusCode();

                var responseStr = await response.Content.ReadAsStringAsync();
                var items = JsonConvert.DeserializeObject<MediaContainer>(responseStr).Items;
                count = items.Count;

                foreach (var item in items)
                {
                    var imdbId = item.ProviderIds?.imdb;
                    if (imdbId != null && !imdb.Contains(imdbId))
                        imdb.Add(imdbId);

                    var tvdbId = item.ProviderIds?.tvdb;
                    if (tvdbId != null && !tvdb.Contains(tvdbId))
                        tvdb.Add(tvdbId);

                    if (imdbId == null && tvdbId == null)
                    {
                        Log.Warning("[GetWatchedMedia]::{0}\t {1} does not have any provider ids", userId, item.Id);
                    }
                }

                Log.Information("[GetWatchedMedia::{0}\tWatched Record Count: {1}", userId, count);
                offset += limit;
            } while (count > 0);

            return (imdb, tvdb);
        }

        private static async Task SetWatchedStatus(Guid userId, HashSet<string> imdb, HashSet<string> tvdb)
        {
            foreach(var id in imdb)
            {
                var dtRow = DestinationMediaTable.AsEnumerable()
                    .FirstOrDefault(row => row.Field<string>("imdbId") == id);

                if (dtRow == null)
                {
                    Log.Warning("[SetWatchedStatus]::{0}\tMedia with IMDB Id: {1} not found", userId, id);
                    return;
                }

                var tvdbId = dtRow.Field<string>("tvdbId");
                if (tvdbId != null && tvdb.Contains(tvdbId))
                    tvdb.Remove(tvdbId);

                await SetWatchedStatusItem(userId, dtRow.Field<string>("id"));
            }

            foreach (var id in tvdb)
            {
                var dtRow = DestinationMediaTable.AsEnumerable()
                    .FirstOrDefault(row => row.Field<string>("tvdbId") == id);
                
                if (dtRow == null)
                {
                    Log.Warning("[SetWatchedStatus]::{0}\tMedia with TVDB Id: {1} not found", userId, id);
                    return;
                }
                
                await SetWatchedStatusItem(userId, dtRow.Field<string>("id"));
            }
        }

        private static async Task SetWatchedStatusItem(Guid userId, string itemId)
        {
            var url = new Uri(
                $"{_destinationUrl}/Users/{userId}/PlayedItems/{itemId}?api_key={_destinationApiKey}");
            
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                RequestUri = url
            };

            var response = await Client.SendAsync(request);
            response.EnsureSuccessStatusCode();
        }
    }
}
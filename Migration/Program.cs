using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using CsvHelper;
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

                Log.Logger = new LoggerConfiguration()
                    .MinimumLevel.Verbose()
                    .WriteTo.File(Path.Join("log", "Migration..log"), rollingInterval: RollingInterval.Day)
                    .WriteTo.Console()
                    .CreateLogger();

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
                DestinationMediaTable.Columns.Add("name", typeof(string));
                DestinationMediaTable.Columns.Add("seriesName", typeof(string));
                DestinationMediaTable.Columns.Add("episodeId", typeof(string));
                await PopulateDestinationMediaAsync(destinationAdminId);

                foreach (var (username, user) in _sourceUsers)
                {
                    if (!_destinationUsers.ContainsKey(username))
                    {
                        Log.Warning("User not found in destination: {0}", username);
                        return;
                    }

                    Log.Information("Starting User: {0}", username);
                    var watchedTable = await GetWatchedMediaAsync(user.Id).ConfigureAwait(false);
                    await SetWatchedStatus(_destinationUsers[username].Id, watchedTable);
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

        /// <summary>
        /// Get destination media.
        /// </summary>
        /// <param name="adminUserId">Admin UserId. Ensures all media is gotten.</param>
        /// <returns></returns>
        private static async Task PopulateDestinationMediaAsync(Guid adminUserId)
        {
            var baseUrl =
                $"{_destinationUrl}/Users/{adminUserId}/Items?Fields=ProviderIds&Recursive=true&IncludeItemTypes=Episode%2CMovie&api_key={_destinationApiKey}";

            int count,
                offset = 0;
            const int limit = 500;

            Directory.CreateDirectory("destinationMedia");
            await using var fileStream = File.OpenWrite($"destinationMedia/{DateTime.UtcNow:s}.csv");
            await using var streamWriter = new StreamWriter(fileStream);
            await using var csvWriter = new CsvWriter(streamWriter, CultureInfo.InvariantCulture);
            csvWriter.WriteField<string>("imdbId");
            csvWriter.WriteField<string>("tvdbId");
            csvWriter.WriteField<string>("id");
            csvWriter.WriteField<string>("name");
            csvWriter.WriteField<string>("seriesName");
            csvWriter.WriteField<string>("episodeId");
            await csvWriter.NextRecordAsync();

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
                    if (!string.IsNullOrEmpty(imdbId) &&
                        !imdbId.StartsWith("tt", StringComparison.OrdinalIgnoreCase))
                    {
                        imdbId = "tt" + imdbId;
                    }

                    var tvdbId = item.ProviderIds?.tvdb;
                    var episodeId = item.IndexNumber.HasValue && item.ParentIndexNumber.HasValue
                        ? $"S{item.ParentIndexNumber!:D2}E{item.IndexNumber!:D2}"
                        : null;

                    var row = DestinationMediaTable.NewRow();
                    row["imdbId"] = imdbId;
                    row["tvdbId"] = tvdbId;
                    row["id"] = item.Id;
                    row["name"] = item.Name;
                    row["seriesName"] = item.SeriesName ?? string.Empty;
                    row["episodeId"] = episodeId;

                    csvWriter.WriteField<string>(imdbId);
                    csvWriter.WriteField<string>(tvdbId);
                    csvWriter.WriteField<string>(item.Id);
                    csvWriter.WriteField<string>(item.Name);
                    csvWriter.WriteField<string>(item.SeriesName);
                    csvWriter.WriteField<string>(episodeId);
                    await csvWriter.NextRecordAsync();
                    DestinationMediaTable.Rows.Add(row);
                }

                Log.Information("[PopulateDestinationMedia]\t Media Count: {itemCount}", count);
                offset += limit;
            } while (count > 0);
        }

        /// <summary>
        /// Get watched media for user.
        /// </summary>
        /// <param name="userId">User to get watched media for.</param>
        /// <returns></returns>
        private static async Task<DataTable> GetWatchedMediaAsync(Guid userId)
        {
            var baseUrl =
                $"{_sourceUrl}/Users/{userId}/Items?Fields=ProviderIds&Recursive=true&IsPlayed=true&api_key={_sourceApiKey}";

            int count,
                offset = 0;
            const int limit = 500;
            
            var watchedTable = new DataTable();
            watchedTable.Columns.Add("imdbId", typeof(string));
            watchedTable.Columns.Add("tvdbId", typeof(string));
            watchedTable.Columns.Add("name", typeof(string));
            watchedTable.Columns.Add("seriesName", typeof(string));
            watchedTable.Columns.Add("episodeId", typeof(string));
            
            do
            {
                var partUrl = baseUrl + $"&StartIndex={offset}&Limit={limit}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                    RequestUri = new Uri(partUrl)
                };

                var success = false;
                HttpResponseMessage response = null;
                do
                {
                    try
                    {
                        response = await Client.SendAsync(request);
                        response.EnsureSuccessStatusCode();
                        success = true;
                    }
                    catch (Exception e)
                    {
                        Log.Warning(e, "[GetWatchedMedia]::{0}\tError: {1}", userId, request.RequestUri);
                        await Task.Delay(5000);
                    }
                } while (!success);
                

                var responseStr = await response.Content.ReadAsStringAsync();
                var items = JsonConvert.DeserializeObject<MediaContainer>(responseStr).Items;
                count = items.Count;

                foreach (var item in items)
                {
                    var imdbId = item.ProviderIds?.imdb;
                    var tvdbId = item.ProviderIds?.tvdb;

                    var watchedRow = watchedTable.NewRow();
                    watchedRow["imdbId"] = imdbId;
                    watchedRow["tvdbId"] = tvdbId;
                    watchedRow["name"] = item.Name;
                    watchedRow["seriesName"] = item.SeriesName;
                    watchedRow["episodeId"] = item.IndexNumber.HasValue && item.ParentIndexNumber.HasValue
                        ? $"S{item.ParentIndexNumber!:D2}E{item.IndexNumber!:D2}"
                        : null;
                    watchedTable.Rows.Add(watchedRow);
                }

                Log.Information("[GetWatchedMedia]::{0}\tWatched Record Count: {1}", userId, count);
                offset += limit;
            } while (count > 0);

            return watchedTable;
        }

        /// <summary>
        /// Set watched status.
        /// </summary>
        /// <param name="userId">User to set watched status for.</param>
        /// <param name="watchedTable">Table of watched status.</param>
        /// <returns></returns>
        private static async Task SetWatchedStatus(Guid userId, DataTable watchedTable)
        {
            var count = 0;
            foreach (DataRow row in watchedTable.Rows)
            {
                var imdbId = row.Field<string>("imdbId");
                var tvdbId = row.Field<string>("tvdbId");
                var name = row.Field<string>("name");
                var seriesName = row.Field<string>("seriesName");
                var episodeId = row.Field<string>("episodeId");

                DataRow matchingRow = null;
                foreach (DataRow destinationRow in DestinationMediaTable.Rows)
                {
                    var destinationImdbId = destinationRow.Field<string>("imdbId");
                    var destinationTvdbId = destinationRow.Field<string>("tvdbId");
                    var destinationName = destinationRow.Field<string>("name");
                    var destinationSeriesName = destinationRow.Field<string>("seriesName");
                    var destinationEpisodeId = destinationRow.Field<string>("episodeId");

                    if (!string.IsNullOrEmpty(imdbId)
                        && !string.IsNullOrEmpty(destinationImdbId)
                        && destinationImdbId.Equals(imdbId, StringComparison.OrdinalIgnoreCase))
                    {
                        matchingRow = destinationRow;
                        break;
                    }

                    if (!string.IsNullOrEmpty(tvdbId) 
                        && !string.IsNullOrEmpty(destinationTvdbId) 
                        && destinationTvdbId.Equals(tvdbId, StringComparison.CurrentCultureIgnoreCase))
                    {
                        matchingRow = destinationRow;
                        break;
                    }

                    if (!string.IsNullOrEmpty(name)
                        && !string.IsNullOrEmpty(destinationName)
                        && destinationName.Equals(name, StringComparison.OrdinalIgnoreCase))
                    {
                        if (string.IsNullOrEmpty(seriesName) && string.IsNullOrEmpty(destinationSeriesName))
                        {
                            matchingRow = destinationRow;
                            break;
                        }

                        if (!string.IsNullOrEmpty(seriesName)
                            && !string.IsNullOrEmpty(destinationSeriesName)
                            && seriesName.Equals(destinationSeriesName, StringComparison.OrdinalIgnoreCase))
                        {
                            matchingRow = destinationRow;
                            break;
                        }
                    }

                    if (!string.IsNullOrEmpty(seriesName)
                        && !string.IsNullOrEmpty(destinationSeriesName)
                        && !string.IsNullOrEmpty(episodeId)
                        && !string.IsNullOrEmpty(destinationEpisodeId)
                        && seriesName.Equals(destinationSeriesName, StringComparison.OrdinalIgnoreCase)
                        && episodeId.Equals(destinationEpisodeId, StringComparison.OrdinalIgnoreCase)
                    )
                    {
                        matchingRow = destinationRow;
                        break;
                    }
                }

                if (matchingRow == null)
                {
                    Log.Warning("[SetWatchedStatus]::{0}\t imdb: {1}, tvdb: {2}, name: {3}, seriesName: {4} not found",
                        userId, imdbId, tvdbId, name, seriesName);
                    continue;
                }
                
                // Count number of matched items
                count++;
                if (count % 500 == 0)
                {
                    Log.Information("[SetWatchedMedia]::{0}\tWatched Record Count: {1}", userId, count);
                }

                await SetWatchedStatusItem(userId, matchingRow.Field<string>("id")).ConfigureAwait(false);
            }
            
            Log.Information("[SetWatchedMedia]::{0}\tTotal Watched Record Count: {1}", userId, count);
        }

        /// <summary>
        /// Set watched status for item.
        /// </summary>
        /// <param name="userId">User to set watched status for.</param>
        /// <param name="itemId">Item to set watched status for.</param>
        /// <returns></returns>
        private static async Task SetWatchedStatusItem(Guid userId, string itemId)
        {
            var success = false;
            do
            {
                try
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
                    success = true;
                }
                catch (Exception e)
                {
                    Log.Warning(e, "[SetWatchedStatusItem]::{0}\tItemId: {1}", userId, itemId);
                    await Task.Delay(5000);
                }
            } while (!success);

        }
    }
}
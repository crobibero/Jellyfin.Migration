using System.Data;
using System.Globalization;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Web;
using Jellyfin.Migration.Models;
using Microsoft.Extensions.Configuration;
using Serilog;

namespace Jellyfin.Migration;

internal static class Program
{
    private const string LastRunFile = "lastrun.log";
    private const string SettingsKey = "Settings";
    private const string SourceUrlKey = "Source:Url";
    private const string SourceApiKeyKey = "Source:ApiKey";
    private const string DestinationUrlKey = "Destination:Url";
    private const string DestinationApiKeyKey = "Destination:ApiKey";
    private const string DestinationAdminUsernameKey = "Destination:AdminUsername";
    private static readonly string RequestHeader = @$"MediaBrowser Client=""Migration"", Device=""Migration Station"", DeviceId=""{Guid.NewGuid()}"", Version=""0.0.1""";

    private static string _sourceUrl;
    private static string _sourceApiKey;
    private static string _destinationUrl;
    private static string _destinationApiKey;

    private static Dictionary<string, User> _sourceUsers;
    private static Dictionary<string, User> _destinationUsers;
        
    private static readonly DataTable DestinationMediaTable = new();

    private static readonly HttpClient Client = new();

    private static readonly JsonSerializerOptions JsonSerializerOptions = new()
    {
        Converters =
        {
            new JsonGuidConverter()
        }
    };

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

            var lastRun = DateTime.MinValue;
            if (File.Exists(LastRunFile))
            {
                var lastRunStr = await File.ReadAllTextAsync(LastRunFile);
                if (DateTime.TryParse(lastRunStr, out lastRun))
                {
                    lastRun = lastRun.AddDays(-1);
                }
            }

            var settings = config.GetSection(SettingsKey);
            _sourceUrl = settings[SourceUrlKey]?.TrimEnd('/');
            _sourceApiKey = settings[SourceApiKeyKey];
            _destinationUrl = settings[DestinationUrlKey]?.TrimEnd('/');
            _destinationApiKey = settings[DestinationApiKeyKey];
            var destinationAdminUsername = settings[DestinationAdminUsernameKey]!;

            Log.Verbose("Configuration: Source Url: {SourceUrl}", _sourceUrl);
            Log.Verbose("Configuration: Source ApiKey: {SourceApiKey}", _sourceApiKey);
            Log.Verbose("Configuration: Destination Url: {DestinationUrl}", _destinationUrl);
            Log.Verbose("Configuration: Destination ApiKey: {DestinationApiKey}", _destinationApiKey);
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
                
            Log.Information("Source User Count: {SourceUserCount}", _sourceUsers.Count);
            Log.Information("Destination User Count: {DestinationUserCount}", _destinationUsers.Count);

            Guid destinationAdminId;
            if (_destinationUsers.ContainsKey(destinationAdminUsername))
            {
                destinationAdminId = _destinationUsers[destinationAdminUsername].Id;
            }
            else
            {
                Log.Error("Destination Admin {DestinationAdminUsername} not found", destinationAdminUsername);
                return;
            }

            DestinationMediaTable.Columns.Add("imdbId", typeof(string));
            DestinationMediaTable.Columns.Add("tvdbId", typeof(string));
            DestinationMediaTable.Columns.Add("id", typeof(string));
            DestinationMediaTable.Columns.Add("name", typeof(string));
            DestinationMediaTable.Columns.Add("Path", typeof(string));
            await PopulateDestinationMediaAsync(destinationAdminId);

            foreach (var (username, user) in _sourceUsers)
            {
                if (!_destinationUsers.ContainsKey(username))
                {
                    Log.Warning("User not found in destination: {Username}", username);
                    continue;
                }

                Log.Information("Starting User: {Username}", username);
                var watchedTable = await GetWatchedMediaAsync(user.Id, lastRun).ConfigureAwait(false);
                await SetWatchedStatus(_destinationUsers[username].Id, watchedTable);
            }
            
            // Update the last run time.
            await File.WriteAllTextAsync(LastRunFile, DateTime.UtcNow.AddHours(-6).ToString("s", CultureInfo.InvariantCulture));
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
            RequestUri = new Uri($"{url}/Users?api_key={apiKey}"),
            Headers =
            {
                { "X-Emby-Authorization", RequestHeader }
            }
        };

        var response = await Client.SendAsync(request);
        response.EnsureSuccessStatusCode();

        var userList = await response.Content.ReadFromJsonAsync<User[]>(JsonSerializerOptions);
        return userList!.ToDictionary(u => u.Name, u => u, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Get destination media.
    /// </summary>
    /// <param name="adminUserId">Admin UserId. Ensures all media is gotten.</param>
    /// <returns></returns>
    private static async Task PopulateDestinationMediaAsync(Guid adminUserId)
    {
        var baseUrl = new StringBuilder()
            .Append(_destinationUrl)
            .Append("/Users/")
            .Append(adminUserId)
            .Append("/Items")
            .Append("?Fields=").Append(HttpUtility.UrlEncode("ProviderIds,Path"))
            .Append("&IncludeItemTypes=").Append(HttpUtility.UrlEncode("Episode,Movie"))
            .Append("&Recursive=true")
            .Append("&api_key=").Append(_destinationApiKey)
            .ToString();

        int count, offset = 0, totalCount = 0;
        const int limit = 500;

        do
        {
            var partUrl = baseUrl + $"&StartIndex={offset}&Limit={limit}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
                RequestUri = new Uri(partUrl),
                Headers =
                {
                    { "X-Emby-Authorization", RequestHeader }
                }
            };

            var response = await Client.SendAsync(request);
            response.EnsureSuccessStatusCode();

            var mediaContainer = await response.Content.ReadFromJsonAsync<MediaContainer>(JsonSerializerOptions);
            var items = mediaContainer!.Items;
            count = items.Count;

            foreach (var item in items)
            {
                var imdbId = item.ProviderIds?.Imdb;
                if (!string.IsNullOrEmpty(imdbId) &&
                    !imdbId.StartsWith("tt", StringComparison.OrdinalIgnoreCase))
                {
                    imdbId = "tt" + imdbId;
                }

                var tvdbId = item.ProviderIds?.Tvdb;
                var row = DestinationMediaTable.NewRow();
                row["imdbId"] = imdbId ?? string.Empty;
                row["tvdbId"] = tvdbId ?? string.Empty;
                row["id"] = item.Id;
                row["name"] = item.Name;
                row["path"] = item.Path; 

                DestinationMediaTable.Rows.Add(row);
            }

            totalCount += count;
            Log.Information("[PopulateDestinationMedia]\t Media Count: {ItemCount}", totalCount);
            offset += limit;
        } while (count > 0);
    }

    /// <summary>
    /// Get watched media for user.
    /// </summary>
    /// <param name="userId">User to get watched media for.</param>
    /// <param name="lastRun">The last time the migration ran.</param>
    /// <returns></returns>
    private static async Task<DataTable> GetWatchedMediaAsync(Guid userId, DateTime lastRun)
    {
        var baseUrl = new StringBuilder()
            .Append(_sourceUrl)
            .Append("/Users/")
            .Append(userId)
            .Append("/Items")
            .Append("?Fields=").Append(HttpUtility.UrlEncode("ProviderIds,Path"))
            .Append("&IncludeItemTypes=").Append(HttpUtility.UrlEncode("Episode,Movie"))
            .Append("&IsPlayed=true")
            .Append("&SortOrder=Descending")
            .Append("&SortBy=DatePlayed")
            .Append("&Recursive=true")
            .Append("&api_key=").Append(_sourceApiKey)
            .ToString();
        
        int count,
            offset = 0;
        const int limit = 500;
            
        var watchedTable = new DataTable();
        watchedTable.Columns.Add("imdbId", typeof(string));
        watchedTable.Columns.Add("tvdbId", typeof(string));
        watchedTable.Columns.Add("path", typeof(string));
        watchedTable.Columns.Add("name", typeof(string));
        watchedTable.Columns.Add("lastPlayedDate", typeof(DateTime));

        var totalCount = 0;
        do
        {
            var partUrl = baseUrl + $"&StartIndex={offset}&Limit={limit}";

            

            var success = false;
            HttpResponseMessage response = null;
            do
            {
                try
                {
                    var request = new HttpRequestMessage
                    {
                        Method = HttpMethod.Get,
                        RequestUri = new Uri(partUrl),
                        Headers =
                        {
                            { "X-Emby-Authorization", RequestHeader }
                        }
                    };
                    
                    response = await Client.SendAsync(request);
                    response.EnsureSuccessStatusCode();
                    success = true;
                }
                catch (Exception e)
                {
                    Log.Warning(e, "[GetWatchedMedia]::{UserId}\tError: {RequestUri}", userId, partUrl);
                    await Task.Delay(5000);
                }
            } while (!success);

            var mediaContainer = await response.Content.ReadFromJsonAsync<MediaContainer>(JsonSerializerOptions);
            var items = mediaContainer!.Items;
            count = items.Count;

            foreach (var item in items)
            {
                var lastPlayedDate = item.UserData?.LastPlayedDate ?? DateTime.UtcNow;
                if (lastPlayedDate < lastRun)
                {
                    Log.Information(
                        "[GetWatchedMedia::{UserId}\tFinished recently played media, {Count} items",
                        userId,
                        totalCount);

                    return watchedTable;
                }

                count++;
                var imdbId = item.ProviderIds?.Imdb;
                var tvdbId = item.ProviderIds?.Tvdb;

                var watchedRow = watchedTable.NewRow();
                watchedRow["imdbId"] = imdbId ?? string.Empty;
                watchedRow["tvdbId"] = tvdbId ?? string.Empty;
                watchedRow["name"] = item.Name;
                watchedRow["lastPlayedDate"] = lastPlayedDate;
                watchedTable.Rows.Add(watchedRow);
            }

            Log.Information("[GetWatchedMedia]::{UserId}\tWatched Record Count: {Count}", userId, totalCount);
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
        var totalCount = 0;
        foreach (DataRow row in watchedTable.Rows)
        {
            var imdbId = row.Field<string>("imdbId");
            var tvdbId = row.Field<string>("tvdbId");
            var path = row.Field<string>("path");
            var name = row.Field<string>("name");
            var lastPlayedDate = row.Field<DateTime>("lastPlayedDate");
            
            DataRow matchingRow = null;
            foreach (DataRow destinationRow in DestinationMediaTable.Rows)
            {
                var destinationImdbId = destinationRow.Field<string>("imdbId");
                var destinationTvdbId = destinationRow.Field<string>("tvdbId");
                var destinationPath = destinationRow.Field<string>("path");

                if (!string.IsNullOrEmpty(path)
                    && !string.IsNullOrEmpty(destinationPath)
                    && string.Equals(path, destinationPath, StringComparison.OrdinalIgnoreCase))
                {
                    matchingRow = destinationRow;
                    break;
                }
                
                if (!string.IsNullOrEmpty(imdbId)
                    && !string.IsNullOrEmpty(destinationImdbId)
                    && string.Equals(destinationImdbId, imdbId, StringComparison.OrdinalIgnoreCase))
                {
                    matchingRow = destinationRow;
                    break;
                }

                if (!string.IsNullOrEmpty(tvdbId) 
                    && !string.IsNullOrEmpty(destinationTvdbId) 
                    && string.Equals(destinationTvdbId, tvdbId, StringComparison.OrdinalIgnoreCase))
                {
                    matchingRow = destinationRow;
                    break;
                }
            }

            if (matchingRow is null)
            {
                Log.Warning("[SetWatchedStatus]::{UserId}\t imdb: {ImdbId}, tvdb: {TvdbId}, name: {Name}, path: {Path} not found",
                    userId, imdbId, tvdbId, name, path);
                continue;
            }
                
            // Count number of matched items
            totalCount++;
            if (totalCount % 25 == 0)
            {
                Log.Information("[SetWatchedMedia]::{UserId}\tWatched Record Count: {Count}", userId, totalCount);
            }

            await SetWatchedStatusItem(
                    userId,
                    matchingRow.Field<string>("id"),
                    lastPlayedDate)
                .ConfigureAwait(false);
        }
            
        Log.Information("[SetWatchedMedia]::{UserId}\tTotal Watched Record Count: {Count}", userId, totalCount);
    }

    /// <summary>
    /// Set watched status for item.
    /// </summary>
    /// <param name="userId">User to set watched status for.</param>
    /// <param name="itemId">Item to set watched status for.</param>
    /// <param name="lastPlayedDate">The date this item was last played.</param>
    /// <returns></returns>
    private static async Task SetWatchedStatusItem(Guid userId, string itemId, DateTime lastPlayedDate)
    {
        var success = false;
        do
        {
            try
            {
                var url = new Uri($"{_destinationUrl}/Users/{userId}/PlayedItems/{itemId}?api_key={_destinationApiKey}");
                var payload = new Dictionary<string, object>
                {
                    ["LastPlayedDate"] = lastPlayedDate
                };

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    RequestUri = url,
                    Headers =
                    {
                        { "X-Emby-Authorization", RequestHeader }
                    },
                    Content = JsonContent.Create(payload)
                };

                var response = await Client.SendAsync(request);
                response.EnsureSuccessStatusCode();
                success = true;
            }
            catch (Exception e)
            {
                Log.Warning(e, "[SetWatchedStatusItem]::{UserId}\tItemId: {ItemId}", userId, itemId);
                await Task.Delay(5000);
            }
        } while (!success);

    }
}

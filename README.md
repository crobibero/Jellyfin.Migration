# Emby.Migration

Migrate watch status from Emby/Jellyfin to Emby/Jellyfin. This will only migrate watched status of users that exist in both the source and destination. 

Items are matched by:
```
imdbId,
tvdbId,
Name (+ SeriesName if show),
SeriesName + S{00}E{00}
```


## Configuration:
Copy `appsettings.Sample.json` to `appsettings.json`

## Running
Run with `dotnet run Migration.dll`
